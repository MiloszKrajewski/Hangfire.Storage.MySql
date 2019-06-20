using System;
using System.Data;
using System.Data.Common;
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
		private readonly string _typeName;

		public MySqlJobQueue(MySqlStorage storage, MySqlStorageOptions options)
		{
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_options = options ?? throw new ArgumentNullException(nameof(options));
			_typeName = GetType().GetFriendlyName();
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
			var prefix = _options.TablesPrefix;
			try
			{
				while (true)
				{
					cancellationToken.ThrowIfCancellationRequested();

					var updated = ClaimJob(connection, prefix, queues, expiration, token);

					if (updated != 0)
					{
						var fetchedJob = FetchJobByToken(connection, prefix, token);
						var hijacked = connection;
						connection = null;
						return new MySqlFetchedJob(_options, hijacked, fetchedJob);
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

		private static int ClaimJob(
			DbConnection connection, string prefix,
			string[] queues, TimeSpan expiration, string token)
		{
			int Action(IContext ctx)
			{
				var now = DateTime.Now;
				var then = now.Subtract(expiration);
				return ctx.C.Execute(
					$@"/* MySqlJobQueue.ClaimJob */
		                update `{ctx.P}JobQueue` 
		                set FetchedAt = @now, FetchToken = @token
		                where (Queue in @queues) and (FetchedAt is null or FetchedAt < @then)
		                limit 1",
					new { queues, now, then, token },
					ctx.T);
			}

			return Repeater
				.Create(connection, prefix)
				.Lock(LockableResource.Queue)
				.Wait(Repeater.Quick)
				.Log(Logger)
				.Execute(Action);
		}

		private static FetchedJob FetchJobByToken(
			IDbConnection connection, string prefix, string token) =>
			connection.QueryFirst<FetchedJob>(
				$@"/* MySqlJobQueue.FetchJobByToken */
                select Id, JobId, Queue
                from `{prefix}JobQueue`
                where FetchToken = @token",
				new { token });

		public void Enqueue(IContext context, string queue, string jobId) =>
			context.C.Execute(
				$@"/* MySqlJobQueue.Enqueue */
                insert into `{context.P}JobQueue` (JobId, Queue)
                values (@jobId, @queue)",
				new { jobId, queue },
				context.T);

		public void Enqueue(IDbConnection connection, string queue, string jobId)
		{
			Repeater
				.Create(connection, _options.TablesPrefix)
				.Lock(LockableResource.Queue)
				.Wait(Repeater.Quick)
				.Log(Logger)
				.Execute(ctx => Enqueue(ctx, queue, jobId));
		}
	}
}
