using System;
using System.Data;
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
		private static readonly string Namespace = typeof(MySqlObjectsInstaller).Namespace;

		public static void Install(MySqlConnection connection, string prefix = null)
		{
			if (connection is null) throw new ArgumentNullException(nameof(connection));

			prefix = prefix ?? string.Empty;
			InstallScript(connection, prefix);
			InstallMigrations(connection, prefix);
		}

		private static void InstallScript(IDbConnection connection, string prefix)
		{
			if (TableExists(connection, prefix, "Job"))
			{
				Log.Info("DB tables already exist. Exit install.");
				return;
			}

			var resourceName = $"{Namespace}.Install.sql";
			var script = GetStringResource(resourceName);
			var formattedScript = GetFormattedScript(script, prefix);

			Log.Info("Start installing Hangfire SQL objects...");

			connection.Execute(formattedScript);

			Log.Info("Hangfire SQL objects installed.");
		}

		private static void InstallMigrations(IDbConnection connection, string prefix)
		{
			using (ResourceLock.AcquireMany(
				connection, prefix, MigrationTimeout, LockableResource.Migration))
			{
				var resourceName = $"{Namespace}.Migrations.xml";
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

		private static void EnsureMigrationsTable(IDbConnection connection, string prefix)
		{
			var tableExists = TableExists(connection, prefix, "Migration");
			if (tableExists) return;

			connection.Execute(
				$@"/* Create migrations table */
                create table {prefix}Migration (
                    Id nvarchar(128) not null, 
                    ExecutedAt datetime(6) not null, 
                    primary key (`Id`)
                ) engine=InnoDB default character set utf8 collate utf8_general_ci;"
			);
		}

		private static bool IsMigrationApplied(IDbConnection connection, string prefix, string id)
		{
			return connection.QueryFirst<int>(
				$"select count(*) from `{prefix}Migration` where Id = @id",
				new { id }) > 0;
		}

		private static void ApplyMigration(
			IDbConnection connection, string script, string prefix, string id)
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

		private static bool TableExists(
			IDbConnection connection, string prefix, string tableName) =>
			connection.ExecuteScalar<string>($"SHOW TABLES LIKE '{prefix}{tableName}';") != null;

		private static string GetStringResource(string resourceName)
		{
			var assembly = typeof(MySqlObjectsInstaller).GetTypeInfo().Assembly;

			using (var stream = assembly.GetManifestResourceStream(resourceName))
			{
				if (stream == null)
				{
					throw new InvalidOperationException(
						String.Format(
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
			sb.Replace("{prefix}", tablesPrefix);
			return sb.ToString();
		}
	}
}
