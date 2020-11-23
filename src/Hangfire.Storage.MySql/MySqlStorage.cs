﻿using System;
using System.Collections.Generic;
using System.Data;
using Hangfire.Logging;
using System.Linq;
using System.Text;
using Hangfire.Annotations;
using Hangfire.Server;
using Hangfire.Storage.MySql.JobQueue;
using Hangfire.Storage.MySql.Locking;
using Hangfire.Storage.MySql.Monitoring;
using MySqlConnector;

namespace Hangfire.Storage.MySql
{
    public class MySqlStorage : JobStorage, IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlStorage));

        private readonly string _connectionString;
        private readonly MySqlStorageOptions _storageOptions;

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public MySqlStorage(string connectionString, MySqlStorageOptions storageOptions)
        {
            if (connectionString == null) throw new ArgumentNullException("connectionString");
            if (storageOptions == null) throw new ArgumentNullException("storageOptions");

            if (IsConnectionString(connectionString))
            {
                _connectionString = ApplyAllowUserVariablesProperty(connectionString);
            }
            else
            {
                throw new ArgumentException(
                    string.Format(
                        "Could not find connection string with name '{0}' in application config file",
                        connectionString));
            }
            _storageOptions = storageOptions;

            if (storageOptions.PrepareSchemaIfNecessary)
            {
                using (var connection = CreateAndOpenConnection())
                {
                    MySqlObjectsInstaller.Install(connection, storageOptions.TablesPrefix);
                    MySqlObjectsInstaller.Upgrade(connection, storageOptions.TablesPrefix);
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
                    new MySqlJobQueueProvider(this, _storageOptions));
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _storageOptions);
            yield return new CountersAggregator(this, _storageOptions);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _storageOptions.QueuePollInterval);
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
            return new MySqlMonitoringApi(this, _storageOptions);
        }

        public override IStorageConnection GetConnection()
        {
            return new MySqlStorageConnection(this, _storageOptions);
        }

        private static bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }

        internal void UseTransaction([InstantHandle] Action<MySqlTransaction> action)
        {
            UseTransaction(connection =>
            {
                action(connection);
                return true;
            }, null);
        }

        internal T UseTransaction<T>(
            [InstantHandle] Func<MySqlTransaction, T> func, IsolationLevel? isolationLevel)
        {
            var connection = CreateAndOpenConnection();
            try
            {
                using (var transaction = connection.BeginTransaction(
                    isolationLevel ?? IsolationLevel.ReadCommitted))
                {
                    var result = func(transaction);
                    transaction.Commit();
                    return result;
                }
            }
            finally
            {
                ReleaseConnection(connection);
            }
        }

        internal void UseConnection([InstantHandle] Action<MySqlConnection> action)
        {
            UseConnection(connection =>
            {
                action(connection);
                return true;
            });
        }

        internal T UseConnection<T>([InstantHandle] Func<MySqlConnection, T> func)
        {
            MySqlConnection connection = null;

            try
            {
                connection = CreateAndOpenConnection();
                return func(connection);
            }
            finally
            {
                ReleaseConnection(connection);
            }
        }

        internal MySqlConnection CreateAndOpenConnection()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();
            ResourceLock.ReleaseAll(connection);
            return connection;
        }

        internal void ReleaseConnection(IDbConnection connection)
        {
            if (connection != null)
            {
                connection.Dispose();
            }
        }
        
        public void Dispose()
        {
        }
    }
}
