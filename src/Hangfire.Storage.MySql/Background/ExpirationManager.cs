using System;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage.MySql.Locking;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql
{
	internal class ExpirationManager: IServerComponent
	{
		private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

		private static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(30);
		private const string LockName = "ExpirationManager";

		private static readonly TimeSpan PassInterval = TimeSpan.FromSeconds(1);
		private const int PassSize = 1000;

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
			var passInterval = PassInterval;
			var batchInterval = _options.JobExpirationCheckInterval;

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

				var (table, resource) = ProcessedTables[index];

				var removed = PurgeTable(cancellationToken, table, resource);

				if (removed > 0)
				{
					// if something was removed we will need to do another batch
					retry = true;
					Logger.TraceFormat(
						"Removed {0} outdated record(s) from '{1}' table.",
						removed, table);
				}

				cancellationToken.WaitHandle.WaitOne(passInterval);
				cancellationToken.ThrowIfCancellationRequested();

				// next item in batch
				index = (index + 1) % ProcessedTables.Length;
			}

			cancellationToken.WaitHandle.WaitOne(batchInterval);
		}

		private int PurgeTable(
			CancellationToken token, string table, LockableResource resource)
		{
			var prefix = _options.TablesPrefix;
			using (var connection = _storage.CreateAndOpenConnection())
			using (AcquireGlobalLock(token, connection, prefix))
			{
				const int count = PassSize;

				return Repeater
					.Create(connection, prefix)
					.Lock(LockableResource.Counter)
					.Wait(token)
					.Log(Logger, $"{GetType().GetFriendlyName()}.PurgeTable({prefix}{table})")
					.Execute(PurgeQuery(prefix, table), new { count });
			}
		}

		private static string PurgeQuery(string prefix, string table) =>
			$"delete from `{prefix}{table}` where ExpireAt < utc_timestamp() limit @count";

		private static IDisposable AcquireGlobalLock(
			CancellationToken token, MySqlConnection connection, string prefix) =>
			_ResourceLock.AcquireAny(connection, prefix, LockTimeout, token, LockName);

		public override string ToString() => GetType().GetFriendlyName();
	}
}
