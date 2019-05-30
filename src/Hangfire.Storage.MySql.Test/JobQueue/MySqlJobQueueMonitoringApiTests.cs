using System;
using System.Linq;
using System.Transactions;
using Dapper;
using Hangfire.Storage.MySql.JobQueue;
using Hangfire.Storage.MySql.Test.Utils;
using Xunit;

namespace Hangfire.Storage.MySql.Test.JobQueue
{
    public class MySqlJobQueueMonitoringApiTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private readonly MySqlJobQueueMonitoringApi _sut;
        private readonly MySqlStorage _storage;
        private readonly string _queue = "default";

        public MySqlJobQueueMonitoringApiTests()
        {
            var storageOptions = new MySqlStorageOptions();
            _storage = new MySqlStorage(ConnectionUtils.GetConnectionString(), storageOptions);
            _sut = new MySqlJobQueueMonitoringApi(_storage, storageOptions);
        }

        public void Dispose()
        {
            _storage.Dispose();
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedAndFetchedCount_ReturnsEqueuedCount_WhenExists()
        {
            EnqueuedAndFetchedCountDto result = null;

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into JobQueue (JobId, Queue) " +
                    "values (1, @queue);", new { queue = _queue });

                result = _sut.GetEnqueuedAndFetchedCount(_queue);

                connection.Execute("delete from JobQueue");
            });

            Assert.Equal(1, result.EnqueuedCount);
        }


        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedJobIds_ReturnsEmptyCollection_IfQueueIsEmpty()
        {
            var result = _sut.GetEnqueuedJobIds(_queue, 5, 15);

            Assert.Empty(result);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedJobIds_ReturnsCorrectResult()
        {
            int[] result = null;
            _storage.UseConnection(connection =>
            {
                for (var i = 1; i <= 10; i++)
                {
                    connection.Execute(
                        "insert into JobQueue (JobId, Queue) " +
                        "values (@jobId, @queue);", new { jobId = i, queue = _queue });
                }

                result = _sut.GetEnqueuedJobIds(_queue, 3, 2).ToArray();

                connection.Execute("delete from JobQueue");
            });

            Assert.Equal(2, result.Length);
            Assert.Equal(4, result[0]);
            Assert.Equal(5, result[1]);
        }
    }
}
