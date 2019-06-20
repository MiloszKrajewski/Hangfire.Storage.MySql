using System;
using System.Collections.Generic;
using Dapper;
using System.Linq;

namespace Hangfire.Storage.MySql.JobQueue
{
	internal class MySqlJobQueueMonitoringApi: IPersistentJobQueueMonitoringApi
	{
		private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);
		private readonly object _cacheLock = new object();
		private string[] _queuesCache = Array.Empty<string>();
		private DateTime _cacheUpdated;

		private readonly MySqlStorage _storage;
		private readonly MySqlStorageOptions _options;

		public MySqlJobQueueMonitoringApi(MySqlStorage storage, MySqlStorageOptions options)
		{
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_options = options ?? throw new ArgumentNullException(nameof(options));
		}

		public IEnumerable<string> GetQueues()
		{
			lock (_cacheLock)
			{
				var empty = !_queuesCache.Any();
				var expired = DateTime.UtcNow >= _cacheUpdated.Add(QueuesCacheTimeout);
				if (!empty && !expired) return _queuesCache;

				using (var connection = _storage.CreateAndOpenConnection())
				{
					var prefix = _options.TablesPrefix;
					_cacheUpdated = DateTime.UtcNow;
					return _queuesCache = connection
						.Query<string>($"select distinct(`Queue`) from `{prefix}JobQueue`")
						.ToArray();
				}
			}
		}

		public IEnumerable<int> GetEnqueuedJobIds(string queue, int offset, int length)
		{
			using (var connection = _storage.CreateAndOpenConnection())
			{
				var prefix = _options.TablesPrefix;
				return connection.Query<int>(
					$@"/* GetEnqueuedJobIds */
					select jq.JobId 
					from `{prefix}JobQueue` jq
					where jq.Queue = @queue
					order by jq.Id
					limit @offset, @length",
					new { queue, offset, length }
				).ToArray();
			}
		}

		public IEnumerable<int> GetFetchedJobIds(string queue, int from, int perPage) =>
			Enumerable.Empty<int>();

		public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
		{
			using (var connection = _storage.CreateAndOpenConnection())
			{
				var prefix = _options.TablesPrefix;
				var result = connection.QueryFirst<int>(
					$"select count(Id) from `{prefix}JobQueue` where Queue = @queue",
					new { queue });
				return new EnqueuedAndFetchedCountDto { EnqueuedCount = result };
			}
		}
	}
}
