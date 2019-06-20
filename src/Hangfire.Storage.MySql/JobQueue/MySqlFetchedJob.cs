using System;
using System.Data.Common;
using System.Globalization;
using Dapper;
using Hangfire.Logging;
using Hangfire.Storage.MySql.Locking;

namespace Hangfire.Storage.MySql.JobQueue
{
	internal class MySqlFetchedJob: IFetchedJob
	{
		private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlFetchedJob));

		private readonly DbConnection _connection;
		private readonly MySqlStorageOptions _options;
		private readonly int _id;
		private bool _removed;
		private bool _requeued;
		private bool _disposed;

		public MySqlFetchedJob(
			MySqlStorageOptions options,
			DbConnection connection,
			FetchedJob fetchedJob)
		{
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

			_disposed = true;

			if (!_removed && !_requeued)
			{
				Requeue();
			}

			_connection?.Dispose();
		}

		public void RemoveFromQueue()
		{
			Logger.TraceFormat("RemoveFromQueue JobId={0}", JobId);

			int Action(IContext ctx) =>
				ctx.C.Execute(
					$"delete from `{ctx.P}JobQueue` where Id = @id",
					new { id = _id },
					ctx.T);

			Repeater
				.Create(_connection, _options.TablesPrefix)
				.Lock(LockableResource.Queue)
				.Wait(Repeater.Long)
				.Log(Logger)
				.Execute(Action);

			_removed = true;
		}

		public void Requeue()
		{
			Logger.TraceFormat("Requeue JobId={0}", JobId);

			int Action(IContext ctx) =>
				ctx.C.Execute(
					$"update `{ctx.P}JobQueue` set FetchedAt = null where Id = @id",
					new { id = _id },
					ctx.T);

			Repeater
				.Create(_connection, _options.TablesPrefix)
				.Lock(LockableResource.Queue)
				.Wait(Repeater.Long)
				.Log(Logger)
				.Execute(Action);

			_requeued = true;
		}

		public string JobId { get; }

		public string Queue { get; }
	}
}
