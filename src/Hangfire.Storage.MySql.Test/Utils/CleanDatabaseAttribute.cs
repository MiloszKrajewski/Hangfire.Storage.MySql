using System.Reflection;
using System.Transactions;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit.Sdk;

namespace Hangfire.Storage.MySql.Test.Utils
{
	public class CleanDatabaseAttribute: BeforeAfterTestAttribute
	{
		private readonly IsolationLevel _isolationLevel;

		public CleanDatabaseAttribute(
			IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
		{
			_isolationLevel = isolationLevel;
		}

		public override void Before(MethodInfo methodUnderTest)
		{
			CreateDatabase();
		}

		public override void After(MethodInfo methodUnderTest)
		{
			PurgeDatabase();
		}

		private static void CreateDatabase()
		{
			PurgeDatabase();
			using (var connection = new MySqlConnection(ConnectionUtils.GetConnectionString()))
			{
				connection.Open();
				MySqlObjectsInstaller.Install(connection);
				MySqlObjectsInstaller.Upgrade(connection);
			}
		}

		private static void PurgeDatabase()
		{
			using (var connection = new MySqlConnection(ConnectionUtils.GetMasterConnectionString()))
			{
				connection.Open();
				var databaseName = ConnectionUtils.GetDatabaseName();
				connection.Execute($"drop database if exists `{databaseName}`");
				connection.Execute($"create database `{databaseName}` collate utf8_bin");
			}
		}
	}
}
