using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage.MySql.Locking;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql
{
    internal class ExpirationManager : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly (string, LockableResource)[] ProcessedTables = {
            ("AggregatedCounter", LockableResource.Counter),
            ("Job", LockableResource.Job),
            ("List", LockableResource.List),
            ("Set", LockableResource.Set),
            ("Hash", LockableResource.Hash)
        }; 

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _storageOptions;

        public ExpirationManager(MySqlStorage storage, MySqlStorageOptions storageOptions)
        {
            _storage = storage ?? throw new ArgumentNullException("storage");
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var tablePrefix = _storageOptions.TablesPrefix;
            foreach (var (tableName, lockType) in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", tableName);

                int removedCount = 0;

                do
                {
                    _storage.UseConnection(connection =>
                    {
                        try
                        {
                            using (ResourceLock.AcquireOne(
                                connection, _storageOptions.TablesPrefix, DefaultLockTimeout, cancellationToken, lockType))
                            {
                                removedCount = connection.Execute(
                                    $"delete from `{tablePrefix}{tableName}` where ExpireAt < @now limit @count",
                                    new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
                            }

                            Logger.DebugFormat("removed records count={0}", removedCount);
                        }
                        catch (MySqlException ex)
                        {
                            Logger.Error(ex.ToString());
                        }
                    });

                    if (removedCount > 0)
                    {
                        Logger.Trace(String.Format("Removed {0} outdated record(s) from '{1}' table.", removedCount,
                            tableName));

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount > 0);
            }

            cancellationToken.WaitHandle.WaitOne(_storageOptions.JobExpirationCheckInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }
    }
}
