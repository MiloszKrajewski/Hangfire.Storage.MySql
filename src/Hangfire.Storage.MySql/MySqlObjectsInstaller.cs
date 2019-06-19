using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Reflection;
using Dapper;
using Hangfire.Logging;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Hangfire.Storage.MySql.Locking;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql
{
    internal static class MySqlObjectsInstaller
    {
        private static readonly TimeSpan MigrationTimeout = TimeSpan.FromMinutes(1);
        private static readonly ILog Log = LogProvider.GetLogger(typeof(MySqlStorage));

        public static void Install(MySqlConnection connection, string tablesPrefix = null)
        {
            if (connection == null) throw new ArgumentNullException("connection");

            var prefix = tablesPrefix ?? string.Empty;

            if (TablesExists(connection, prefix))
            {
                Log.Info("DB tables already exist. Exit install");
                return;
            }
            
            var resourceName = $"{typeof(MySqlObjectsInstaller).Namespace}.Install.sql";
            var script = GetStringResource(resourceName);
            var formattedScript = GetFormattedScript(script, prefix);

            Log.Info("Start installing Hangfire SQL objects...");

            connection.Execute(formattedScript);

            Log.Info("Hangfire SQL objects installed.");
        }

        public static void Upgrade(MySqlConnection connection, string tablesPrefix = null)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));

            var prefix = tablesPrefix ?? string.Empty;

            using (ResourceLock.AcquireMany(
                connection, prefix, MigrationTimeout, LockableResource.Migration))
            {
                var resourceName = $"{typeof(MySqlObjectsInstaller).Namespace}.Migrations.xml";
                var document = XElement.Parse(GetStringResource(resourceName));

                EnsureMigrationsTable(connection, prefix);

                var migrations = document
                    .Elements("migration")
                    .Select(e => new { id = e.Attribute("id")?.Value, script = e.Value })
                    .ToArray();

                foreach (var migration in migrations)
                {
                    var alreadyApplied = IsMigrationApplied(connection, prefix, migration.id);
                    if (alreadyApplied) continue;

                    ApplyMigration(connection, migration.script, prefix, migration.id);
                }
            }
        }

        private static void EnsureMigrationsTable(DbConnection connection, string prefix)
        {
            var tableExists = connection.ExecuteScalar<string>($"SHOW TABLES LIKE '{prefix}Migration';") != null;
            if (tableExists) return;

            connection.Execute(
                $@"/* Create migrations table */
                create table {prefix}Migration (
                    Id nvarchar(128) not null, 
                    ExecutedAt datetime(6) not null, 
                    primary key (`Id`)
                ) engine=InnoDB default character set utf8 collate utf8_general_ci;");
        }
        
        private static bool IsMigrationApplied(IDbConnection connection, string prefix, string id)
        {
            return connection.QueryFirst<int>(
                $"select count(*) from `{prefix}Migration` where Id = @id",
                new { id }) > 0;
        }
        
        private static void ApplyMigration(
            DbConnection connection, string script, string prefix, string id)
        {
            // NOTE: Some operations cannot be executed in transactions (create table?)
            // we will need some mechanism to handle that if we need it
            using (var transaction = connection.BeginTransaction())
            {
                connection.Execute(
                    GetFormattedScript(script, prefix),
                    null,
                    transaction);
                connection.Execute(
                    $"insert into `{prefix}Migration` (Id, ExecutedAt) values (@id, @now)",
                    new { id, now = DateTime.UtcNow },
                    transaction);
                transaction.Commit();
            }
        }

        private static bool TablesExists(MySqlConnection connection, string tablesPrefix) => 
            connection.ExecuteScalar<string>($"SHOW TABLES LIKE '{tablesPrefix}Job';") != null;

        private static string GetStringResource(string resourceName)
        {
#if NET45
            var assembly = typeof(MySqlObjectsInstaller).Assembly;
#else
            var assembly = typeof(MySqlObjectsInstaller).GetTypeInfo().Assembly;
#endif

            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new InvalidOperationException(String.Format(
                        "Requested resource `{0}` was not found in the assembly `{1}`.",
                        resourceName,
                        assembly));
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }

        private static string GetFormattedScript(string script, string tablesPrefix)
        {
            var sb = new StringBuilder(script);
            sb.Replace("[tablesPrefix]", tablesPrefix);

            return sb.ToString();
        }
    }
}