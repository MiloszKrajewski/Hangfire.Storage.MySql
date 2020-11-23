using System;
using System.Collections.Generic;
using System.Data;
using Hangfire.Logging;
using System.Linq;
using Hangfire.Server;
using Hangfire.Storage.MySql.Install;
using Hangfire.Storage.MySql.JobQueue;
using Hangfire.Storage.MySql.Locking;
using Hangfire.Storage.MySql.Monitoring;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql
{
	public class MySqlStorage: JobStorage, IDisposable
	{
		private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlStorage));

		private readonly string _connectionString;
		private readonly string _cachedToString;
		private readonly MySqlStorageOptions _options;
		private readonly Pool<MySqlConnection> _pool;
		private readonly DistributedLocks _locks;

		internal PersistentJobQueueProviderCollection QueueProviders { get; private set; }

		public MySqlStorage(string connectionString, MySqlStorageOptions options)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));
			_connectionString = FixConnectionString(
				connectionString ?? throw new ArgumentNullException(nameof(connectionString)));
			_cachedToString = BuildToString();

			if (options.PrepareSchemaIfNecessary)
			{
				using (var connection = CreateConnection())
				{
					MySqlObjectsInstaller.Install(connection, options.TablesPrefix);
				}
			}

			InitializeQueueProviders();

			_pool = new Pool<MySqlConnection>(
				4, 16, CreateConnection, RecycleConnection);
			_locks = new DistributedLocks(this, options);
		}

		private static string FixConnectionString(string connectionString)
		{
			string AddPart(string key, string value, string cs) =>
				cs.ToLower().Contains(key.ToLower()) ? cs : $"{cs};{key}={value}";

			return connectionString
				.PipeTo(x => AddPart("Allow User Variables", "true", x));
		}

		private void InitializeQueueProviders()
		{
			QueueProviders =
				new PersistentJobQueueProviderCollection(
					new MySqlJobQueueProvider(this, _options));
		}

		public override IEnumerable<IServerComponent> GetComponents()
		{
			yield return new ExpirationManager(this, _options);
			yield return new CountersAggregator(this, _options);
		}

		public override void WriteOptionsToLog(ILog logger)
		{
			logger.Info("Using the following options for SQL Server job storage:");
			logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
		}

		public override IMonitoringApi GetMonitoringApi() =>
			new MySqlMonitoringApi(this, _options);

		public override IStorageConnection GetConnection() =>
			new MySqlStorageConnection(this, _options);

		private MySqlConnection CreateConnection()
		{
			var connection = new MySqlConnection(_connectionString);
			connection.Open();
			return connection;
		}

		internal ILease<MySqlConnection> BorrowConnection() =>
			_pool.Borrow();

		private static bool RecycleConnection(IDbConnection c)
		{
			ConnectionLock.ReleaseAll(c, null);
			return true;
		}

		internal IDisposable AcquireLock(string resource, TimeSpan timeout) =>
			_locks.Acquire(resource, timeout);

		public void Dispose()
		{
			_locks.TryDispose();
			_pool.TryDispose();
		}

		#region ToString

		public override string ToString() => _cachedToString;

		private static readonly string[] ServerKeys =
			{ "Data Source", "Server", "Address", "Addr", "Network Address" };

		private static readonly string[] DatabaseKeys =
			{ "Database", "Initial Catalog" };

		private string BuildToString()
		{
			var parts = _connectionString
				.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
				.Select(x => x.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries))
				.Select(x => new { Key = x[0].Trim(), Value = x[1].Trim() })
				.ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);

			var serverName = ExtractPart(parts, ServerKeys) ?? "<unknown>";
			var databaseName = ExtractPart(parts, DatabaseKeys) ?? "<unknown>";

			return $"{GetType().GetFriendlyName()}({databaseName}@{serverName})";
		}

		private static string ExtractPart(
			IDictionary<string, string> parts, string[] aliases) =>
			aliases.Select(k => parts.TryGetOrDefault(k)).FirstOrDefault(v => v != null);

		#endregion
	}
}
