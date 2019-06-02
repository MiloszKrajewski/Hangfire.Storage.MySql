using System;
using System.Data;
using Dapper;
using Hangfire.Logging;
using System.Linq;
using System.Threading;
using Hangfire.Storage.MySql.Locking;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql.JobQueue
{
	internal class MySqlJobQueue: IPersistentJobQueue
	{
		private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlJobQueue));

		private readonly MySqlStorage _storage;
		private readonly MySqlStorageOptions _options;

		public MySqlJobQueue(MySqlStorage storage, MySqlStorageOptions options)
		{
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_options = options ?? throw new ArgumentNullException(nameof(options));
		}

		public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
		{
			if (queues is null || !queues.Any())
				throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

			var token = Guid.NewGuid().ToString();
			// I'm not sure if this is used correctly...
			var expiration = _options.InvisibilityTimeout;
			// the connection variable is mutable, it job is claimed then this connection
			// is passed to job itself, and variable is set to null so it does not get
			// disposed here in such case
			var connection = _storage.CreateAndOpenConnection();
			try
			{
				while (true)
				{
					cancellationToken.ThrowIfCancellationRequested();

					var updated = ClaimJob(connection, queues, expiration, token);

					if (updated != 0)
					{
						var fetchedJob = FetchJobByToken(connection, token);
						return new MySqlFetchedJob(
							_storage, _options,
							Interlocked.Exchange(ref connection, null),
							fetchedJob);
					}

					cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
					cancellationToken.ThrowIfCancellationRequested();
				}
			}
			catch (MySqlException ex)
			{
				Logger.ErrorException(ex.Message, ex);
				throw;
			}
			finally
			{
				connection?.Dispose();
			}
		}

		private int ClaimJob(
			IDbConnection connection, string[] queues, TimeSpan expiration, string token)
		{
			return Deadlock.Retry(
				() => {
					var now = DateTime.Now;
					var then = now.Subtract(expiration);
					return connection.Execute(
						$@"/* MySqlJobQueue.ClaimJob */
		                update `{_options.TablesPrefix}JobQueue` 
		                set FetchedAt = @now, FetchToken = @token
		                where (Queue in @queues) and (FetchedAt is null or FetchedAt < @then)
		                limit 1",
						new { queues, now, then, token });
				}, Logger);
		}

		private FetchedJob FetchJobByToken(IDbConnection connection, string token)
		{
			return connection.QueryFirst<FetchedJob>(
				$@"/* MySqlJobQueue.FetchJobByToken */
                select Id, JobId, Queue
                from `{_options.TablesPrefix}JobQueue`
                where FetchToken = @token",
				new { token });
		}

		private void Enqueue(
			IDbConnection connection, IDbTransaction transaction, string queue, string jobId)
		{
			Logger.TraceFormat("Enqueue JobId={0} Queue={1}", jobId, queue);

			connection.Execute(
				$@"/* MySqlJobQueue.Enqueue */
                insert into `{_options.TablesPrefix}JobQueue` (JobId, Queue)
                values (@jobId, @queue)",
				new { jobId, queue },
				transaction);
		}

		public void Enqueue(IDbConnection connection, string queue, string jobId) =>
			Deadlock.Retry(() => Enqueue(connection, null, queue, jobId), Logger);

		public void Enqueue(IDbTransaction transaction, string queue, string jobId) =>
			Enqueue(transaction.Connection, transaction, queue, jobId);

	}
}
