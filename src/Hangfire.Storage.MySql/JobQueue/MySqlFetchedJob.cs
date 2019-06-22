using System;
using System.Data;
using System.Globalization;

namespace Hangfire.Storage.MySql.JobQueue
{
	internal class MySqlFetchedJob: IFetchedJob
	{
		private readonly MySqlJobQueue _queue;
		private readonly IDbConnection _connection;
		private readonly int _id;
		private bool _removed;
		private bool _requeued;
		private bool _disposed;

		public MySqlFetchedJob(
			MySqlJobQueue queue,
			IDbConnection connection,
			FetchedJob fetchedJob)
		{
			_queue = queue ?? throw new ArgumentNullException(nameof(queue));
			_connection = connection ?? throw new ArgumentNullException(nameof(connection));

			if (fetchedJob == null)
				throw new ArgumentNullException(nameof(fetchedJob));

			_id = fetchedJob.Id;
			JobId = fetchedJob.JobId.ToString(CultureInfo.InvariantCulture);
			Queue = fetchedJob.Queue;
		}

		public void Dispose()
		{
			if (_disposed) return;

			_disposed = true;

			if (!_removed && !_requeued)
			{
				Requeue();
			}

			_connection?.Dispose();
		}

		public void RemoveFromQueue()
		{
			_queue.Remove(_connection, _id);
			_removed = true;
		}

		public void Requeue()
		{

			_queue.Requeue(_connection, _id);
			_requeued = true;
		}

		public string JobId { get; }

		public string Queue { get; }
	}
}
