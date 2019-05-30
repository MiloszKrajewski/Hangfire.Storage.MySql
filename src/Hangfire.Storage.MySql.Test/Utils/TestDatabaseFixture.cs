using System;
using System.Threading;
using Dapper;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql.Test.Utils
{
    public class TestDatabaseFixture : IDisposable
    {
        private static readonly object GlobalLock = new object();
        public TestDatabaseFixture()
        {
            Monitor.Enter(GlobalLock);
            CreateAndInitializeDatabase();
        }
        public void Dispose()
        {
            DropDatabase();
            Monitor.Exit(GlobalLock);
        }

        private static void CreateAndInitializeDatabase()
        {
            var recreateDatabaseSql = String.Format(
                @"CREATE DATABASE IF NOT EXISTS `{0}`",
                ConnectionUtils.GetDatabaseName());

            using (var connection = new MySqlConnection(
                ConnectionUtils.GetMasterConnectionString()))
            {
                connection.Execute(recreateDatabaseSql);
            }

            using (var connection = new MySqlConnection(
                ConnectionUtils.GetConnectionString()))
            {
				connection.Open();
                MySqlObjectsInstaller.Install(connection);
                MySqlObjectsInstaller.Upgrade(connection);
            }
        }

        private static void DropDatabase()
        {
            var recreateDatabaseSql = String.Format(
                   @"DROP DATABASE IF EXISTS `{0}`",
                   ConnectionUtils.GetDatabaseName());

            using (var connection = new MySqlConnection(
                ConnectionUtils.GetMasterConnectionString()))
            {
                connection.Execute(recreateDatabaseSql);
            }
        }
    }
}
