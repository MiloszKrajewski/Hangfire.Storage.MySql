using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage.MySql.Locking;

namespace Hangfire.Storage.MySql
{
    internal class CountersAggregator : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(CountersAggregator));

        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _storageOptions;

        public CountersAggregator(MySqlStorage storage, MySqlStorageOptions storageOptions)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            _storage = storage;
            _storageOptions = storageOptions;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            Logger.DebugFormat($"Aggregating records in '{_storageOptions.TablesPrefix}Counter' table...");

            int removedCount = 0;

            do
            {
                _storage.UseConnection(connection =>
                {
                    using (ResourceLock.AcquireOne(
                        connection, _storageOptions.TablesPrefix, LockableResource.Counter))
                    {
                        removedCount = connection.Execute(
                            GetAggregationQuery(),
                            new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
                    }
                });

                if (removedCount >= NumberOfRecordsInSinglePass)
                {
                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount >= NumberOfRecordsInSinglePass);

            cancellationToken.WaitHandle.WaitOne(_storageOptions.CountersAggregateInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }

        private string GetAggregationQuery()
        {
            var prefix = _storageOptions.TablesPrefix;
            return $@"
                create temporary table __refs__ engine = memory as
                    select `Id` from `lib1_Counter` limit @count;

                insert into `{prefix}AggregatedCounter` (`Key`, Value, ExpireAt)
                    select `Key`, SumValue, MaxExpireAt
                    from (
                        select `Key`, sum(Value) as SumValue, max(ExpireAt) AS MaxExpireAt
                        from `{prefix}Counter` c join __refs__ r on (r.Id = c.Id)
                        group by `Key`
                    ) _
                    on duplicate key update
                        Value = Value + values(Value),
                        ExpireAt = greatest(ExpireAt, values(ExpireAt));

                delete c from `{prefix}Counter` c join __refs__ r on (r.Id = c.Id);

                drop table __refs__;
            ";
		}
	}
}
