using System;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Storage.MySql.Test.Utils;
using Xunit;

namespace Hangfire.Storage.MySql.Test
{
    public class CountersAggregatorTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private readonly CountersAggregator _sut;
        private readonly MySqlStorage _storage;

        public CountersAggregatorTests()
        {
            var options = new MySqlStorageOptions
            {
                CountersAggregateInterval = TimeSpan.Zero
            };
            _storage = new MySqlStorage(ConnectionUtils.GetConnectionString(), options);
            _sut = new CountersAggregator(_storage, options);
        }
        public void Dispose()
        {
            _storage.Dispose();
        }

        [Fact, CleanDatabase]
        public void CountersAggregatorExecutesProperly()
        {
            const string createSql = @"
insert into Counter (`Key`, Value, ExpireAt) 
values ('key', 1, @expireAt)";

            _storage.UseConnection(connection =>
            {
                // Arrange
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddHours(1) });

                var cts = new CancellationTokenSource();
                cts.Cancel();

                // Act
                _sut.Execute(cts.Token);

                // Assert
                Assert.Equal(1, connection.Query<int>(@"select count(*) from AggregatedCounter").Single());
            });
        }
    }
}
