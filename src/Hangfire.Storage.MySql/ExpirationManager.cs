using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage.MySql.Locking;
using MySqlConnector;

namespace Hangfire.Storage.MySql
{
	internal class ExpirationManager: IServerComponent
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
		private readonly MySqlStorageOptions _options;

		public ExpirationManager(MySqlStorage storage, MySqlStorageOptions options)
		{
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_options = options ?? throw new ArgumentNullException(nameof(options));
		}

		public void Execute(CancellationToken cancellationToken)
		{
			var tablePrefix = _options.TablesPrefix;
			var retry = true;
			var index = 0;

			while (true)
			{
				// starting new batch
				if (index == 0)
				{
					// if previous one did not delete anything we do not need to retry
					if (!retry) break;

					// start new batch, assume it will be last run
					retry = false;
				}

				var (tableName, lockType) = ProcessedTables[index];

				Logger.DebugFormat("Removing outdated records from table '{0}'...", tableName);

				var removedCount = _storage.UseConnection(
					connection => RemoveExpiredRows(
						cancellationToken, connection, tablePrefix, tableName, lockType));

				if (removedCount > 0)
				{
					// if something was removed we will need to do another batch
					retry = true;
					Logger.TraceFormat(
						"Removed {0} outdated record(s) from '{1}' table.",
						removedCount, tableName);
				}

				cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
				cancellationToken.ThrowIfCancellationRequested();
				
				// next item in batch
				index = (index + 1) % ProcessedTables.Length;
			}

			cancellationToken.WaitHandle.WaitOne(_options.JobExpirationCheckInterval);
		}

		private int RemoveExpiredRows(
			CancellationToken cancellationToken, MySqlConnection connection,
			string tablePrefix, string tableName, LockableResource lockType)
		{
			try
			{
				using (ResourceLock.AcquireOne(
					connection, _options.TablesPrefix, DefaultLockTimeout, cancellationToken,
					lockType))
				{
					var removedCount = connection.Execute(
						$"delete from `{tablePrefix}{tableName}` where ExpireAt < @now limit @count",
						new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
					Logger.DebugFormat("removed records count={0}", removedCount);
					return removedCount;
				}
			}
			catch (MySqlException ex)
			{
				Logger.Error(ex.ToString());
				return 0;
			}
		}

		public override string ToString() { return GetType().ToString(); }
	}
}
