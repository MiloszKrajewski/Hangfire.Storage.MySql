using System;
using System.Data;
using System.Globalization;
using Dapper;
using Hangfire.Logging;
using Hangfire.Storage.MySql.Locking;

namespace Hangfire.Storage.MySql.JobQueue
{
	internal class MySqlFetchedJob: IFetchedJob
	{
		private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlFetchedJob));

		private readonly MySqlStorage _storage;
		private readonly IDbConnection _connection;
		private readonly MySqlStorageOptions _options;
		private readonly int _id;
		private bool _removed;
		private bool _requeued;
		private bool _disposed;

		public MySqlFetchedJob(
			MySqlStorage storage,
			MySqlStorageOptions options,
			IDbConnection connection,
			FetchedJob fetchedJob)
		{
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_connection = connection ?? throw new ArgumentNullException(nameof(connection));
			_options = options ?? throw new ArgumentNullException(nameof(options));

			if (fetchedJob == null)
				throw new ArgumentNullException(nameof(fetchedJob));

			_id = fetchedJob.Id;
			JobId = fetchedJob.JobId.ToString(CultureInfo.InvariantCulture);
			Queue = fetchedJob.Queue;
		}

		public void Dispose()
		{
			if (_disposed) return;

			if (!_removed && !_requeued)
			{
				Requeue();
			}

			_storage.ReleaseConnection(_connection);

			_disposed = true;
		}

		public void RemoveFromQueue()
		{
			Logger.TraceFormat("RemoveFromQueue JobId={0}", JobId);

			using (ResourceLock.AcquireOne(
				_connection, _options.TablesPrefix, LockableResource.Queue))
			{
				_connection.Execute(
					$"delete from `{_options.TablesPrefix}JobQueue` where Id = @id",
					new { id = _id });
			}

			_removed = true;
		}

		public void Requeue()
		{
			Logger.TraceFormat("Requeue JobId={0}", JobId);

			using (ResourceLock.AcquireOne(
				_connection, _options.TablesPrefix, LockableResource.Queue))
			{
				_connection.Execute(
					$"update `{_options.TablesPrefix}JobQueue` set FetchedAt = null where Id = @id",
					new { id = _id });
			}

			_requeued = true;
		}

		public string JobId { get; }

		public string Queue { get; }
	}
}
