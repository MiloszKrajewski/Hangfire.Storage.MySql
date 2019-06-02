using System;
using System.Collections.Generic;
using Dapper;
using Hangfire.Logging;
using System.Linq;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage.MySql.Locking;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql
{
    internal class MySqlWriteOnlyTransaction : JobStorageTransaction
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlWriteOnlyTransaction));

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _storageOptions;

        private readonly Queue<Action<MySqlTransaction>> _commandQueue
            = new Queue<Action<MySqlTransaction>>();

        public MySqlWriteOnlyTransaction(MySqlStorage storage, MySqlStorageOptions storageOptions)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            _storage = storage;
            _storageOptions = storageOptions;
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            Logger.TraceFormat("ExpireJob jobId={0}",jobId);

            AcquireJobLock();

            QueueCommand(x => 
                x.Connection.Execute(
                    $"update `{_storageOptions.TablesPrefix}Job` set ExpireAt = @expireAt where Id = @id",
                    new { expireAt = DateTime.UtcNow.Add(expireIn), id = jobId },
                    x));
        }
        
        public override void PersistJob(string jobId)
        {
            Logger.TraceFormat("PersistJob jobId={0}", jobId);

            AcquireJobLock();

            QueueCommand(x => 
                x.Connection.Execute(
                    $"update `{_storageOptions.TablesPrefix}Job` set ExpireAt = NULL where Id = @id",
                    new { id = jobId },
                    x));
        }

        public override void SetJobState(string jobId, IState state)
        {
            Logger.TraceFormat("SetJobState jobId={0}", jobId);

            AcquireStateLock();
            AcquireJobLock();
            QueueCommand(x => x.Connection.Execute(
                $"insert into `{_storageOptions.TablesPrefix}State` (JobId, Name, Reason, CreatedAt, Data) " +
                "values (@jobId, @name, @reason, @createdAt, @data); " +
                $"update `{_storageOptions.TablesPrefix}Job` set StateId = last_insert_id(), StateName = @name where Id = @id;",
                new
                {
                    jobId = jobId,
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData()),
                    id = jobId
                },
                x));
        }

        public override void AddJobState(string jobId, IState state)
        {
            Logger.TraceFormat("AddJobState jobId={0}, state={1}", jobId, state);

            AcquireStateLock();
            QueueCommand(x => x.Connection.Execute(
                $"insert into `{_storageOptions.TablesPrefix}State` (JobId, Name, Reason, CreatedAt, Data) " +
                "values (@jobId, @name, @reason, @createdAt, @data)",
                new {
                    jobId = jobId,
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData())
                },
                x));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            Logger.TraceFormat("AddToQueue jobId={0}", jobId);

            var provider = _storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();

            QueueCommand(x => persistentQueue.Enqueue(x, queue, jobId));
        }

        public override void IncrementCounter(string key)
        {
            Logger.TraceFormat("IncrementCounter key={0}", key);

            AcquireCounterLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"insert into `{_storageOptions.TablesPrefix}Counter` (`Key`, `Value`) values (@key, @value)",
                    new { key, value = +1 },
                    x));
            
        }


        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            Logger.TraceFormat("IncrementCounter key={0}, expireIn={1}", key, expireIn);

            AcquireCounterLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"insert into `{_storageOptions.TablesPrefix}Counter` (`Key`, `Value`, `ExpireAt`) values (@key, @value, @expireAt)",
                    new { key, value = +1, expireAt = DateTime.UtcNow.Add(expireIn) },
                    x));
        }

        public override void DecrementCounter(string key)
        {
            Logger.TraceFormat("DecrementCounter key={0}", key);

            AcquireCounterLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"insert into `{_storageOptions.TablesPrefix}Counter` (`Key`, `Value`) values (@key, @value)",
                    new { key, value = -1 },
                    x));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            Logger.TraceFormat("DecrementCounter key={0} expireIn={1}", key, expireIn);

            AcquireCounterLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"insert into `{_storageOptions.TablesPrefix}Counter` (`Key`, `Value`, `ExpireAt`) values (@key, @value, @expireAt)",
                    new { key, value = -1, expireAt = DateTime.UtcNow.Add(expireIn) },
                    x));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            Logger.TraceFormat("AddToSet key={0} value={1}", key, value);

            AcquireSetLock();
            QueueCommand(x => x.Connection.Execute(
                $"INSERT INTO `{_storageOptions.TablesPrefix}Set` (`Key`, `Value`, `Score`) " +
                "VALUES (@key, @value, @score) " +
                "ON DUPLICATE KEY UPDATE `Score` = @score",
                new { key, value, score },
                x));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            Logger.TraceFormat("AddRangeToSet key={0}", key);

            if (key == null) throw new ArgumentNullException("key");
            if (items == null) throw new ArgumentNullException("items");

            AcquireSetLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"insert into `{_storageOptions.TablesPrefix}Set` (`Key`, Value, Score) values (@key, @value, 0.0)", 
                    items.Select(value => new { key = key, value = value }).ToList(),
                    x));
        }


        public override void RemoveFromSet(string key, string value)
        {
            Logger.TraceFormat("RemoveFromSet key={0} value={1}", key, value);

            AcquireSetLock();
            QueueCommand(x => x.Connection.Execute(
                $"delete from `{_storageOptions.TablesPrefix}Set` where `Key` = @key and Value = @value",
                new { key, value },
                x));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            Logger.TraceFormat("ExpireSet key={0} expirein={1}", key, expireIn);

            if (key == null) throw new ArgumentNullException("key");

            AcquireSetLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"update `{_storageOptions.TablesPrefix}Set` set ExpireAt = @expireAt where `Key` = @key", 
                    new { key = key, expireAt = DateTime.UtcNow.Add(expireIn) },
                    x));
        }

        public override void InsertToList(string key, string value)
        {
            Logger.TraceFormat("InsertToList key={0} value={1}", key, value);

            AcquireListLock();
            QueueCommand(x => x.Connection.Execute(
                $"insert into `{_storageOptions.TablesPrefix}List` (`Key`, Value) values (@key, @value)",
                new { key, value },
                x));
        }


        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException("key");

            Logger.TraceFormat("ExpireList key={0} expirein={1}", key, expireIn);

            AcquireListLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"update `{_storageOptions.TablesPrefix}List` set ExpireAt = @expireAt where `Key` = @key", 
                    new { key = key, expireAt = DateTime.UtcNow.Add(expireIn) },
                    x));
        }

        public override void RemoveFromList(string key, string value)
        {
            Logger.TraceFormat("RemoveFromList key={0} value={1}", key, value);

            AcquireListLock();
            QueueCommand(x => x.Connection.Execute(
                $"delete from `{_storageOptions.TablesPrefix}List` where `Key` = @key and Value = @value",
                new { key, value },
                x));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            Logger.TraceFormat("TrimList key={0} from={1} to={2}", key, keepStartingFrom, keepEndingAt);

            AcquireListLock();
            QueueCommand(x => x.Connection.Execute(
                $@"/* trim list */
                delete lst
                from `{_storageOptions.TablesPrefix}List` lst
	                inner join (SELECT tmp.Id, @rownum := @rownum + 1 AS `rank`
		  		                FROM `{_storageOptions.TablesPrefix}List` tmp, 
       				                (SELECT @rownum := 0) r ) ranked on ranked.Id = lst.Id
                where lst.Key = @key
                    and ranked.`rank` not between @start and @end",
                new { key = key, start = keepStartingFrom + 1, end = keepEndingAt + 1 },
                x));
        }

        public override void PersistHash(string key)
        {
            Logger.TraceFormat("PersistHash key={0} ", key);

            if (key == null) throw new ArgumentNullException("key");

            AcquireHashLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"update `{_storageOptions.TablesPrefix}Hash` set ExpireAt = null where `Key` = @key", 
                    new { key = key },
                    x));
        }

        public override void PersistSet(string key)
        {
            Logger.TraceFormat("PersistSet key={0} ", key);

            if (key == null) throw new ArgumentNullException("key");

            AcquireSetLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"update `{_storageOptions.TablesPrefix}Set` set ExpireAt = null where `Key` = @key", 
                    new { key = key },
                    x));
        }

        public override void RemoveSet(string key)
        {
            Logger.TraceFormat("RemoveSet key={0} ", key);

            if (key == null) throw new ArgumentNullException("key");

            AcquireSetLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"delete from `{_storageOptions.TablesPrefix}Set` where `Key` = @key", 
                    new { key = key },
                    x));
        }

        public override void PersistList(string key)
        {
            Logger.TraceFormat("PersistList key={0} ", key);

            if (key == null) throw new ArgumentNullException("key");

            AcquireListLock();
            QueueCommand(x => x.Connection.Execute(
                    $"update `{_storageOptions.TablesPrefix}List` set ExpireAt = null where `Key` = @key", 
                    new { key = key },
                    x));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            Logger.TraceFormat("SetRangeInHash key={0} ", key);

            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            AcquireHashLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $@"/* SetRangeInHash */
                    insert into `{_storageOptions.TablesPrefix}Hash` (`Key`, Field, Value) " +
                    "values (@key, @field, @value) " +
                    "on duplicate key update Value = @value",
                    keyValuePairs.Select(y => new { key = key, field = y.Key, value = y.Value }),
                    x));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            Logger.TraceFormat("ExpireHash key={0} ", key);

            if (key == null) throw new ArgumentNullException("key");

            AcquireHashLock();
            QueueCommand(x => 
                x.Connection.Execute(
                    $"update `{_storageOptions.TablesPrefix}Hash` set ExpireAt = @expireAt where `Key` = @key", 
                    new { key = key, expireAt = DateTime.UtcNow.Add(expireIn) },
                    x));
        }

        public override void RemoveHash(string key)
        {
            Logger.TraceFormat("RemoveHash key={0} ", key);

            if (key == null) throw new ArgumentNullException("key");

            AcquireHashLock();
            QueueCommand(x => x.Connection.Execute(
                $"delete from `{_storageOptions.TablesPrefix}Hash` where `Key` = @key", 
                new { key },
                x));
        }

        public override void Commit()
        {
            _storage.UseTransaction(transaction => {
                foreach (var command in _commandQueue)
                {
                    command(transaction);
                }
            });
        }

        internal void QueueCommand(Action<MySqlTransaction> action)
        {
            _commandQueue.Enqueue(action);
        }
        
        private void AcquireLock(LockableResource _)
        {
            // no locks are acquired, this solution rely on 'deadlock exception' and 'retry'
            // it may lead to starvation, that's why it is experimental
        }
        
        private void AcquireJobLock()
        {
            AcquireLock(LockableResource.Job);
        }

        private void AcquireSetLock()
        {
            AcquireLock(LockableResource.Set);
        }
        
        private void AcquireListLock()
        {
            AcquireLock(LockableResource.List);
        }

        private void AcquireHashLock()
        {
            AcquireLock(LockableResource.Hash);
        }
        
        private void AcquireStateLock()
        {
            AcquireLock(LockableResource.State);
        }

        private void AcquireCounterLock()
        {
            AcquireLock(LockableResource.Counter);
       }
    }
}
