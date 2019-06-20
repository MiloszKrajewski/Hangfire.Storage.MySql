using System;
using System.Collections.Generic;
using Dapper;
using Hangfire.Logging;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage.MySql.Entities;
using Hangfire.Storage.MySql.Locking;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql
{
	public class MySqlStorageConnection: JobStorageConnection
	{
		private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlStorageConnection));

		private readonly MySqlStorage _storage;
		private readonly MySqlStorageOptions _options;
		private readonly string _prefix;

		public MySqlStorageConnection(MySqlStorage storage, MySqlStorageOptions options)
		{
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_options = options ?? throw new ArgumentNullException(nameof(options));
			_prefix = options.TablesPrefix;
		}

		public override IWriteOnlyTransaction CreateWriteTransaction() =>
			new MySqlWriteOnlyTransaction(_storage, _options);

		public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
		{
			var connection = NewConnection();
			try
			{
				return DisposableBag.Create(
					ResourceLock.AcquireAny(connection, _prefix, timeout, resource),
					connection);
			}
			catch
			{
				connection.Dispose();
				throw;
			}
		}

		public override string CreateExpiredJob(
			Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
		{
			if (job == null) throw new ArgumentNullException(nameof(job));
			if (parameters == null) throw new ArgumentNullException(nameof(parameters));

			var invocationData = ToInvocationData(job);
			var serializedData = ToJson(invocationData);

			Logger.TraceFormat("CreateExpiredJob={0}", serializedData);

			string Action(IContext ctx)
			{
				var jobId = ctx.C.Query<int>(
					$@"/* CreateExpiredJob */
					insert into `{ctx.P}Job` (InvocationData, Arguments, CreatedAt, ExpireAt) 
					values (@invocationData, @arguments, @createdAt, @expireAt);
					select last_insert_id();",
					new {
						invocationData = serializedData,
						arguments = invocationData.Arguments,
						createdAt,
						expireAt = createdAt.Add(expireIn)
					}, ctx.T).Single().ToString();

				if (parameters.Count <= 0) return jobId;

				var parameterArray = parameters
					.Select(p => new { jobId, name = p.Key, value = p.Value })
					.ToArray();

				ctx.C.Execute(
					$@"/* CreateExpiredJob */
					insert into `{ctx.P}JobParameter` (JobId, Name, Value) 
					values (@jobId, @name, @value)",
					parameterArray,
					ctx.T);

				return jobId;
			}

			using (var connection = NewConnection())
			{
				return Repeater
					.Create(connection, _prefix)
					.Lock(LockableResource.Job)
					.Wait(Repeater.Quick)
					.Log(Logger)
					.ExecuteMany(Action);
			}
		}

		public override IFetchedJob FetchNextJob(
			string[] queues, CancellationToken cancellationToken)
		{
			if (queues == null || queues.Length == 0)
				throw new ArgumentNullException(nameof(queues));

			var providers = queues
				.Select(_storage.QueueProviders.GetProvider)
				.Distinct()
				.ToArray();

			if (providers.Length != 1)
			{
				throw new InvalidOperationException(
					string.Format(
						"Multiple provider instances registered for queues: {0}. " +
						"You should choose only one type of persistent queues per server instance.",
						string.Join(", ", queues)));
			}

			var queue = providers.First().GetJobQueue();
			return queue.Dequeue(queues, cancellationToken);
		}

		public override void SetJobParameter(string id, string name, string value)
		{
			if (id == null) throw new ArgumentNullException(nameof(id));
			if (name == null) throw new ArgumentNullException(nameof(name));

			using (var connection = NewConnection())
			{
				Repeater
					.Create(connection, _prefix)
					.Lock(LockableResource.Job)
					.Wait(Repeater.Quick)
					.Log(Logger)
					.Execute(
						$@"/* MySqlStorageConnection.SetJobParameter */
	                    insert into `[prefix]JobParameter` (JobId, Name, Value) 
	                    value (@jobId, @name, @value) 
	                    on duplicate key update Value = @value",
						new { jobId = id, name, value });
			}
		}

		public override string GetJobParameter(string id, string name)
		{
			if (id == null) throw new ArgumentNullException(nameof(id));
			if (name == null) throw new ArgumentNullException(nameof(name));

			using (var connection = NewConnection())
			{
				return connection.Query<string>(
						$@"/* GetJobParameter */
						select Value from `{_prefix}JobParameter` 
						where JobId = @id and Name = @name",
						new { id, name })
					.SingleOrDefault();
			}
		}

		public override JobData GetJobData(string jobId)
		{
			if (jobId is null)
				throw new ArgumentNullException(nameof(jobId));

			using (var connection = NewConnection())
			{
				var job = connection.Query<SqlJob>(
						$@"/* GetJobData */
						select InvocationData, StateName, Arguments, CreatedAt 
						from `{_prefix}Job` 
						where Id = @id",
						new { id = jobId })
					.SingleOrDefault();

				if (job is null) return null;

				var result = new JobData { State = job.StateName, CreatedAt = job.CreatedAt };

				try
				{
					var invocationData = FromJson(job);
					invocationData.Arguments = job.Arguments;
					result.Job = FromInvocationData(invocationData);
				}
				catch (JobLoadException ex)
				{
					result.LoadException = ex;
				}

				return result;
			}
		}

		public override StateData GetStateData(string jobId)
		{
			if (jobId is null) throw new ArgumentNullException(nameof(jobId));

			using (var connection = NewConnection())
			{
				var state = connection.Query<SqlState>(
						$@"/* GetStateData */
						select s.Name, s.Reason, s.Data 
						from `{_prefix}State` s join `{_prefix}Job` j on j.StateId = s.Id 
						where j.Id = @jobId",
						new { jobId })
					.SingleOrDefault();

				if (state is null) return null;

				var data = new Dictionary<string, string>(
					FromJson<Dictionary<string, string>>(state.Data),
					StringComparer.OrdinalIgnoreCase);

				return new StateData { Name = state.Name, Reason = state.Reason, Data = data };
			}
		}

		public override void AnnounceServer(string serverId, ServerContext context)
		{
			if (serverId == null) throw new ArgumentNullException(nameof(serverId));
			if (context == null) throw new ArgumentNullException(nameof(context));

			using (var connection = NewConnection())
			{
				var serverData = new ServerData {
					WorkerCount = context.WorkerCount,
					Queues = context.Queues,
					StartedAt = DateTime.UtcNow,
				};

				Repeater
					.Create(connection, _prefix)
					.Lock(LockableResource.Server)
					.Wait(Repeater.Quick)
					.Log(Logger)
					.Execute(
						$@"/* AnnounceServer */
						insert into `{_prefix}Server` (Id, Data, LastHeartbeat) 
						value (@id, @data, @heartbeat) 
						on duplicate key update Data = @data, LastHeartbeat = @heartbeat",
						new {
							id = serverId,
							data = ToJson(serverData),
							heartbeat = DateTime.UtcNow
						});
			}
		}

		public override void RemoveServer(string serverId)
		{
			if (serverId == null) throw new ArgumentNullException(nameof(serverId));

			using (var connection = NewConnection())
			{
				Repeater
					.Create(connection, _options.TablesPrefix)
					.Lock(LockableResource.Server)
					.Wait(Repeater.Quick)
					.Log(Logger)
					.Execute(
						$"delete from `{_prefix}Server` where Id = @id",
						new { id = serverId });
			}
		}

		public override void Heartbeat(string serverId)
		{
			if (serverId == null) throw new ArgumentNullException(nameof(serverId));

			using (var connection = NewConnection())
			{
				Repeater
					.Create(connection, _prefix)
					.Lock(LockableResource.Server)
					.Wait(Repeater.Quick)
					.Log(Logger)
					.Execute(
						$"update `{_prefix}Server` set LastHeartbeat = @now where Id = @id",
						new { now = DateTime.UtcNow, id = serverId });
			}
		}

		public override int RemoveTimedOutServers(TimeSpan timeout)
		{
			if (timeout.TotalSeconds < 0)
				throw new ArgumentException(
					"The `timeout` value must be positive.", nameof(timeout));

			using (var connection = NewConnection())
			{
				return Repeater
					.Create(connection, _prefix)
					.Lock(LockableResource.Server)
					.Wait(Repeater.Quick)
					.Log(Logger)
					.Execute(
						$"delete from `{_prefix}Server` where LastHeartbeat < @timeoutAt",
						new { timeoutAt = DateTime.UtcNow.Add(timeout.Negate()) });
			}
		}

		public override long GetSetCount(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				return connection.QueryFirst<int>(
					$"select count(`Key`) from `{_prefix}Set` where `Key` = @key",
					new { key });
			}
		}

		public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				return connection.Query<string>(
					$@"/* GetRangeFromSet */
					select `Value` 
					from (
						select `Value`, @rownum := @rownum + 1 AS `rank`
						from `{_prefix}Set`, (select @rownum := 0) r 
						where `Key` = @key
						order by Id
					) ranked
					where ranked.`rank` between @startingFrom and @endingAt",
					new { key, startingFrom = startingFrom + 1, endingAt = endingAt + 1 }
				).ToList();
			}
		}

		public override HashSet<string> GetAllItemsFromSet(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				var result = connection.Query<string>(
					$"select Value from `{_prefix}Set` where `Key` = @key",
					new { key });

				return new HashSet<string>(result);
			}
		}

		public override string GetFirstByLowestScoreFromSet(
			string key, double fromScore, double toScore)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));
			if (toScore < fromScore)
				throw new ArgumentException(
					"The `toScore` value must be higher or equal to the `fromScore` value.");

			using (var connection = NewConnection())
			{
				return connection.Query<string>(
						$@"select Value from `{_prefix}Set` 
						where `Key` = @key and Score between @from and @to 
						order by Score limit 1",
						new { key, from = fromScore, to = toScore })
					.SingleOrDefault();
			}
		}

		public override long GetCounter(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				return connection.Query<long?>(
					$@"/* GetCounter */
					select sum(s.`Value`) 
					from (
						select sum(`Value`) as `Value` from `{_prefix}Counter` where `Key` = @key
						union all
						select `Value` from `{_prefix}AggregatedCounter` where `Key` = @key
					) as s",
					new { key }).Single() ?? 0;
			}
		}

		public override long GetHashCount(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				return connection.Query<long>(
					$"select count(Id) from `{_prefix}Hash` where `Key` = @key",
					new { key }).Single();
			}
		}

		public override TimeSpan GetHashTtl(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				var result = connection.Query<DateTime?>(
					$"select min(ExpireAt) from `{_prefix}Hash` where `Key` = @key",
					new { key }).Single();
				return GetTtl(result);
			}
		}

		public override long GetListCount(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				return connection.Query<long>(
					$"select count(Id) from `{_prefix}List` where `Key` = @key",
					new { key }).Single();
			}
		}

		public override TimeSpan GetListTtl(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				var result = connection.Query<DateTime?>(
					$"select min(ExpireAt) from `{_prefix}List` where `Key` = @key",
					new { key }).Single();
				return GetTtl(result);
			}
		}

		public override string GetValueFromHash(string key, string name)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));
			if (name == null) throw new ArgumentNullException(nameof(name));

			using (var connection = NewConnection())
			{
				return connection.Query<string>(
					$@"/* GetValueFromHash */
					select `Value` from `{_prefix}Hash` 
					where `Key` = @key and `Field` = @field",
					new { key, field = name }).SingleOrDefault();
			}
		}

		public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				return connection.Query<string>(
					$@"
					select `Value` 
					from (
						select `Value`, @rownum := @rownum + 1 AS `rank`
						from `{_prefix}List`, (select @rownum := 0) r
						where `Key` = @key
						order by Id desc
					) ranked
					where ranked.`rank` between @startingFrom and @endingAt",
					new { key, startingFrom = startingFrom + 1, endingAt = endingAt + 1 }
				).ToList();
			}
		}

		public override List<string> GetAllItemsFromList(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				return connection.Query<string>(
					$@"/* GetAllItemsFromList */
					select `Value` from `{_prefix}List`
					where `Key` = @key order by Id desc",
					new { key }
				).ToList();
			}
		}

		public override TimeSpan GetSetTtl(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				var result = connection.Query<DateTime?>(
					$"select min(ExpireAt) from `{_prefix}Set` where `Key` = @key",
					new { key }).Single();
				return GetTtl(result);
			}
		}

		public override void SetRangeInHash(
			string key, IEnumerable<KeyValuePair<string, string>> values)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));
			if (values == null) throw new ArgumentNullException(nameof(values));

			var cachedValues = values as KeyValuePair<string, string>[] ?? values.ToArray();

			void Action(IContext ctx)
			{
				foreach (var kv in cachedValues)
				{
					ctx.C.Execute(
						$@"/* SetRangeInHash */
                        insert into `{ctx.P}Hash` (`Key`, Field, Value)
						value (@key, @field, @value)
						on duplicate key update Value = @value",
						new { key, field = kv.Key, value = kv.Value },
						ctx.T);
				}
			}

			using (var connection = NewConnection())
			{
				Repeater
					.Create(connection, _options.TablesPrefix)
					.Lock(LockableResource.Hash)
					.Wait(Repeater.Quick)
					.Log(Logger)
					.ExecuteMany(Action);
			}
		}

		public override Dictionary<string, string> GetAllEntriesFromHash(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			using (var connection = NewConnection())
			{
				var result = connection.Query<SqlHash>(
					$"select Field, Value from `{_prefix}Hash` where `Key` = @key",
					new { key }
				).ToDictionary(x => x.Field, x => x.Value);

				return result.Count != 0 ? result : null;
			}
		}
		
		private MySqlConnection NewConnection() => _storage.CreateAndOpenConnection();

		private static TimeSpan GetTtl(DateTime? expiration) =>
			!expiration.HasValue
				? TimeSpan.FromSeconds(-1)
				: expiration.Value - DateTime.UtcNow;

		private static InvocationData FromJson(SqlJob job) =>
			FromJson<InvocationData>(job.InvocationData);

		private static string ToJson(object data) =>
			JobHelper.ToJson(data);

		private static T FromJson<T>(string json) =>
			JobHelper.FromJson<T>(json);

		private static InvocationData ToInvocationData(Job job) =>
			InvocationData.Serialize(job);

		private static Job FromInvocationData(InvocationData invocationData) =>
			invocationData.Deserialize();
	}
}
