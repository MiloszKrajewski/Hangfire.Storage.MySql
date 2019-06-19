using System;
using System.Collections.Generic;
using Dapper;
using Hangfire.Logging;
using System.Linq;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage.MySql.Locking;

namespace Hangfire.Storage.MySql
{
	internal class MySqlWriteOnlyTransaction: JobStorageTransaction
	{
		private static readonly ILog Logger =
			LogProvider.GetLogger(typeof(MySqlWriteOnlyTransaction));

		private readonly MySqlStorage _storage;
		private readonly MySqlStorageOptions _options;

		private readonly HashSet<LockableResource> _resources = new HashSet<LockableResource>();
		private readonly Queue<Action<IContext>> _queue = new Queue<Action<IContext>>();

		public MySqlWriteOnlyTransaction(MySqlStorage storage, MySqlStorageOptions options)
		{
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_options = options ?? throw new ArgumentNullException(nameof(options));
		}

		public override void ExpireJob(string jobId, TimeSpan expireIn)
		{
			Logger.TraceFormat("ExpireJob jobId={0}", jobId);

			var expireAt = DateTime.UtcNow.Add(expireIn);

			QueueCommand(
				LockableResource.Job,
				x => x.C.Execute(
					$"update `{x.Prefix}Job` set ExpireAt = @expireAt where Id = @id",
					new { id = jobId, expireAt },
					x.T));
		}

		public override void PersistJob(string jobId)
		{
			Logger.TraceFormat("PersistJob jobId={0}", jobId);

			QueueCommand(
				LockableResource.Job,
				x => x.C.Execute(
					$"update `{x.Prefix}Job` set ExpireAt = null where Id = @id",
					new { id = jobId },
					x.T));
		}

		public override void SetJobState(string jobId, IState state)
		{
			Logger.TraceFormat("SetJobState jobId={0}", jobId);

			AcquireLock(LockableResource.Job, LockableResource.State);
			QueueCommand(
				x => x.C.Execute(
					$@"/* SetJobState */
					insert into `{x.Prefix}State` (JobId, Name, Reason, CreatedAt, Data)
						values (@jobId, @name, @reason, @createdAt, @data);
					update `{x.Prefix}Job` 
						set StateId = last_insert_id(), StateName = @name 
						where Id = @jobId;",
					new {
						jobId,
						name = state.Name,
						reason = state.Reason,
						createdAt = DateTime.UtcNow,
						data = Serialize(state)
					},
					x.T));
		}

		public override void AddJobState(string jobId, IState state)
		{
			Logger.TraceFormat("AddJobState jobId={0}, state={1}", jobId, state);

			QueueCommand(
				LockableResource.State,
				x => x.C.Execute(
					$@"
					insert into `{x.Prefix}State` (JobId, Name, Reason, CreatedAt, Data)
					values (@jobId, @name, @reason, @createdAt, @data)",
					new {
						jobId,
						name = state.Name,
						reason = state.Reason,
						createdAt = DateTime.UtcNow,
						data = Serialize(state)
					},
					x.T));
		}

		public override void AddToQueue(string queue, string jobId)
		{
			Logger.TraceFormat("AddToQueue jobId={0}", jobId);

			var provider = _storage.QueueProviders.GetProvider(queue);
			var persistentQueue = provider.GetJobQueue();

			QueueCommand(
				LockableResource.Queue,
				x => persistentQueue.Enqueue(x, queue, jobId));
		}

		public override void IncrementCounter(string key)
		{
			Logger.TraceFormat("IncrementCounter key={0}", key);

			QueueCommand(
				LockableResource.Counter,
				x => x.C.Execute(
					$@"/* IncrementCounter */
					insert into `{x.Prefix}Counter` (`Key`, `Value`) values (@key, @value)",
					new { key, value = 1 },
					x.T));
		}

		public override void IncrementCounter(string key, TimeSpan expireIn)
		{
			Logger.TraceFormat("IncrementCounter key={0}, expireIn={1}", key, expireIn);

			var expireAt = DateTime.UtcNow.Add(expireIn);
			QueueCommand(
				LockableResource.Counter,
				x => x.C.Execute(
					$@"/* IncrementCounter */
					insert into `{x.Prefix}Counter` (`Key`, `Value`, `ExpireAt`) 
					values (@key, @value, @expireAt)",
					new { key, value = 1, expireAt },
					x.T));
		}

		public override void DecrementCounter(string key)
		{
			Logger.TraceFormat("DecrementCounter key={0}", key);

			QueueCommand(
				LockableResource.Counter,
				x => x.C.Execute(
					$@"/* DecrementCounter */
					insert into `{x.Prefix}Counter` (`Key`, `Value`) values (@key, @value)",
					new { key, value = -1 },
					x.T));
		}

		public override void DecrementCounter(string key, TimeSpan expireIn)
		{
			Logger.TraceFormat("DecrementCounter key={0} expireIn={1}", key, expireIn);

			var expireAt = DateTime.UtcNow.Add(expireIn);

			QueueCommand(
				LockableResource.Counter,
				x => x.C.Execute(
					$@"/* DecrementCounter */
					insert into `{x.Prefix}Counter` (`Key`, `Value`, `ExpireAt`) 
					values (@key, @value, @expireAt)",
					new { key, value = -1, expireAt },
					x.T));
		}

		public override void AddToSet(string key, string value) =>
			AddToSet(key, value, 0.0);

		public override void AddToSet(string key, string value, double score)
		{
			Logger.TraceFormat("AddToSet key={0} value={1}", key, value);

			QueueCommand(
				LockableResource.Set,
				x => x.C.Execute(
					$@"/* AddToSet */
					insert into `{x.Prefix}Set` (`Key`, `Value`, `Score`) 
					values (@key, @value, @score) 
					on duplicate key update `Score` = @score",
					new { key, value, score },
					x.T));
		}

		public override void AddRangeToSet(string key, IList<string> items)
		{
			Logger.TraceFormat("AddRangeToSet key={0}", key);

			if (key == null) throw new ArgumentNullException(nameof(key));
			if (items == null) throw new ArgumentNullException(nameof(items));

			QueueCommand(
				LockableResource.Set,
				x => x.C.Execute(
					$@"/* AddRangeToSet */
					insert into `{x.Prefix}Set` (`Key`, Value, Score) 
					values (@key, @value, 0.0)",
					items.Select(value => new { key, value }).ToList(),
					x.T));
		}

		public override void RemoveFromSet(string key, string value)
		{
			Logger.TraceFormat("RemoveFromSet key={0} value={1}", key, value);

			QueueCommand(
				LockableResource.Set,
				x => x.C.Execute(
					$@"/* RemoveFromSet */
					delete from `{x.Prefix}Set` where `Key` = @key and Value = @value",
					new { key, value },
					x.T));
		}

		public override void ExpireSet(string key, TimeSpan expireIn)
		{
			Logger.TraceFormat("ExpireSet key={0} expireIn={1}", key, expireIn);

			if (key == null) throw new ArgumentNullException("key");

			var expireAt = DateTime.UtcNow.Add(expireIn);

			QueueCommand(
				LockableResource.Set,
				x => x.C.Execute(
					$@"update `{x.Prefix}Set` set ExpireAt = @expireAt where `Key` = @key",
					new { key, expireAt },
					x.T));
		}

		public override void InsertToList(string key, string value)
		{
			Logger.TraceFormat("InsertToList key={0} value={1}", key, value);

			QueueCommand(
				LockableResource.List,
				x => x.C.Execute(
					$"insert into `{x.Prefix}List` (`Key`, Value) values (@key, @value)",
					new { key, value },
					x.T));
		}

		public override void ExpireList(string key, TimeSpan expireIn)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			Logger.TraceFormat("ExpireList key={0} expireIn={1}", key, expireIn);

			var expireAt = DateTime.UtcNow.Add(expireIn);

			QueueCommand(
				LockableResource.List,
				x => x.C.Execute(
					$"update `{x.Prefix}List` set ExpireAt = @expireAt where `Key` = @key",
					new { key, expireAt },
					x.T));
		}

		public override void RemoveFromList(string key, string value)
		{
			Logger.TraceFormat("RemoveFromList key={0} value={1}", key, value);

			QueueCommand(
				LockableResource.List,
				x => x.C.Execute(
					$"delete from `{x.Prefix}List` where `Key` = @key and Value = @value",
					new { key, value },
					x.T));
		}

		public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
		{
			Logger.TraceFormat(
				"TrimList key={0} from={1} to={2}", key, keepStartingFrom, keepEndingAt);

			QueueCommand(
				LockableResource.List,
				x => x.C.Execute(
					$@"/* TrimList */
	                delete lst
	                from `{x.Prefix}List` lst join (
						select tmp.Id, @rownum := @rownum + 1 as `rank`
						from `{x.Prefix}List` tmp, (select @rownum := 0) r
					) ranked on ranked.Id = lst.Id
	                where lst.Key = @key and ranked.`rank` not between @start and @end",
					new { key, start = keepStartingFrom + 1, end = keepEndingAt + 1 },
					x.T));
		}

		public override void PersistHash(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			Logger.TraceFormat("PersistHash key={0}", key);

			QueueCommand(
				LockableResource.Hash,
				x => x.C.Execute(
					$"update `{x.Prefix}Hash` set ExpireAt = null where `Key` = @key",
					new { key },
					x.T));
		}

		public override void PersistSet(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			Logger.TraceFormat("PersistSet key={0}", key);

			QueueCommand(
				LockableResource.Set,
				x => x.C.Execute(
					$"update `{x.Prefix}Set` set ExpireAt = null where `Key` = @key",
					new { key },
					x.T));
		}

		public override void RemoveSet(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			Logger.TraceFormat("RemoveSet(key:{0})", key);

			QueueCommand(
				LockableResource.Set,
				x => x.C.Execute(
					$"delete from `{x.Prefix}Set` where `Key` = @key",
					new { key },
					x.T));
		}

		public override void PersistList(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			Logger.TraceFormat("PersistList(key:{0})", key);

			QueueCommand(
				LockableResource.List,
				x => x.C.Execute(
					$"update `{x.Prefix}List` set ExpireAt = null where `Key` = @key",
					new { key },
					x.T));
		}

		public override void SetRangeInHash(
			string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
		{
			Logger.TraceFormat("SetRangeInHash(key:{0})", key);

			if (key == null) throw new ArgumentNullException(nameof(key));
			if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

			QueueCommand(
				LockableResource.Hash,
				x => x.C.Execute(
					$@"/* SetRangeInHash */
					insert into `{x.Prefix}Hash` (`Key`, Field, Value) 
					values (@key, @field, @value) 
					on duplicate key update Value = @value",
					keyValuePairs.Select(y => new { key, field = y.Key, value = y.Value }),
					x.T));
		}

		public override void ExpireHash(string key, TimeSpan expireIn)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			Logger.TraceFormat("ExpireHash(key:{0})", key);

			var expireAt = DateTime.UtcNow.Add(expireIn);

			QueueCommand(
				LockableResource.Hash,
				x => x.C.Execute(
					$@"update `{x.Prefix}Hash` set ExpireAt = @expireAt where `Key` = @key",
					new { key, expireAt },
					x.T));
		}

		public override void RemoveHash(string key)
		{
			if (key == null) throw new ArgumentNullException(nameof(key));

			Logger.TraceFormat("RemoveHash key={0}", key);

			QueueCommand(
				LockableResource.Hash,
				x => x.C.Execute(
					$"delete from `{x.Prefix}Hash` where `Key` = @key",
					new { key },
					x.T));
		}

		public override void Commit()
		{
			void Action(IContext c)
			{
				foreach (var command in _queue)
				{
					command(c);
				}
			}

			using (var connection = _storage.CreateAndOpenConnection())
			{
				Repeater
					.Create(connection, _options.TablesPrefix)
					.Lock(_resources.ToArray())
					.Wait(Repeater.Long)
					.Log(Logger)
					.ExecuteMany(Action);
			}
		}

		private void QueueCommand(Action<IContext> action) => 
			_queue.Enqueue(action);

		private void QueueCommand(LockableResource resource, Action<IContext> action)
		{
			AcquireLock(resource);
			_queue.Enqueue(action);
		}

		private void AcquireLock(LockableResource resource) =>
			_resources.Add(resource);

		private void AcquireLock(params LockableResource[] resources)
		{
			foreach (var r in resources) _resources.Add(r);
		}

		private static string Serialize(IState state)
		{
			return JobHelper.ToJson(state.SerializeData());
		}
	}
}
