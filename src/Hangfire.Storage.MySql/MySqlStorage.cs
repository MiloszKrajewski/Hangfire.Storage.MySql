using System;
using System.Collections.Generic;
using Hangfire.Logging;
using System.Linq;
using System.Text;
using Hangfire.Annotations;
using Hangfire.Server;
using Hangfire.Storage.MySql.JobQueue;
using Hangfire.Storage.MySql.Locking;
using Hangfire.Storage.MySql.Monitoring;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql
{
    public class MySqlStorage : JobStorage, IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlStorage));

        private readonly string _connectionString;
        private readonly MySqlStorageOptions _options;

        internal virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public MySqlStorage(string connectionString, MySqlStorageOptions options)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString)); 
            _options = options ?? throw new ArgumentNullException(nameof(options));

            _connectionString = ApplyAllowUserVariablesProperty(_connectionString);

            if (options.PrepareSchemaIfNecessary)
            {
                using (var connection = CreateAndOpenConnection())
                {
                    MySqlObjectsInstaller.Install(connection, options.TablesPrefix);
                    MySqlObjectsInstaller.Upgrade(connection, options.TablesPrefix);
                }
            }
            
            InitializeQueueProviders();
        }
        
        private static string ApplyAllowUserVariablesProperty(string connectionString)
        {
            if (connectionString.ToLower().Contains("allow user variables"))
            {
                return connectionString;
            }

            return connectionString + ";Allow User Variables=True;";
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

        public override string ToString()
        {
            const string canNotParseMessage = "<Connection string can not be parsed>";

            try
            {
                var parts = _connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries))
                    .Select(x => new { Key = x[0].Trim(), Value = x[1].Trim() })
                    .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);

                var builder = new StringBuilder();

                foreach (var alias in new[] { "Data Source", "Server", "Address", "Addr", "Network Address" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                if (builder.Length != 0) builder.Append("@");

                foreach (var alias in new[] { "Database", "Initial Catalog" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                return builder.Length != 0
                    ? String.Format("Server: {0}", builder)
                    : canNotParseMessage;
            }
            catch (Exception ex)
            {
                Logger.ErrorException(ex.Message, ex);
                return canNotParseMessage;
            }
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new MySqlMonitoringApi(this, _options);
        }

        public override IStorageConnection GetConnection()
        {
            return new MySqlStorageConnection(this, _options);
        }

//        internal void UseTransaction([InstantHandle] Action<MySqlTransaction> action)
//        {
//            UseTransaction(connection => {
//                action(connection);
//                return true;
//            });
//        }
//        
//        internal T UseTransaction<T>(
//            [InstantHandle] Func<MySqlTransaction, T> func)
//        {
//            using (var connection = CreateAndOpenConnection())
//            using (var transaction = connection.BeginTransaction())
//            {
//                var result = func(transaction);
//                transaction.Commit();
//                return result;
//            }
//        }

       
        internal T UseConnection<T>([InstantHandle] Func<MySqlConnection, T> func)
        {
            using (var connection = CreateAndOpenConnection())
                return func(connection);
        }

        internal void UseConnection([InstantHandle] Action<MySqlConnection> action)
        {
            UseConnection(connection =>
            {
                action(connection);
                return true;
            });
        }

        internal MySqlConnection CreateAndOpenConnection()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();
            _ResourceLock.ReleaseAll(connection, null);
            return connection;
        }

        public void Dispose()
        {
        }
    }
}
