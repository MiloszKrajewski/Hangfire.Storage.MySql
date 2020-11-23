using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage.MySql.Locking;
using MySqlConnector;

namespace Hangfire.Storage.MySql
{
	internal class CountersAggregator: IServerComponent
	{
		private static readonly ILog Logger = LogProvider.GetLogger(typeof(CountersAggregator));

		private const int NumberOfRecordsInSinglePass = 1000;
		private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

		private readonly MySqlStorage _storage;
		private readonly MySqlStorageOptions _options;

		public CountersAggregator(MySqlStorage storage, MySqlStorageOptions options)
		{
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_options = options ?? throw new ArgumentNullException(nameof(options));
		}

		public void Execute(CancellationToken cancellationToken)
		{
			Logger.DebugFormat(
				$"Aggregating records in '{_options.TablesPrefix}Counter' table...");

			while (true)
			{
				var removedCount = _storage.UseConnection(AggregateCounter);

				if (removedCount < NumberOfRecordsInSinglePass)
					break;

				cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
				cancellationToken.ThrowIfCancellationRequested();
			}

			cancellationToken.WaitHandle.WaitOne(_options.CountersAggregateInterval);
		}

		private int AggregateCounter(MySqlConnection connection)
		{
			using (ResourceLock.AcquireOne(
				connection, _options.TablesPrefix, LockableResource.Counter))
			{
				return connection.Execute(
					GetAggregationQuery(),
					new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
			}
		}

		private string GetAggregationQuery()
		{
			var prefix = _options.TablesPrefix;
			return $@"
                create temporary table __refs__ engine = memory as
                    select `Id` from `{prefix}Counter` limit @count;

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

		public override string ToString() => GetType().ToString();
	}
}
