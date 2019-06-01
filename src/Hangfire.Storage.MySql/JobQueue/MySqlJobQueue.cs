using System;
using System.Data;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Storage.MySql.Locking;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql.JobQueue
{
    internal class MySqlJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlJobQueue));

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _options;
        public MySqlJobQueue(MySqlStorage storage, MySqlStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (options == null) throw new ArgumentNullException("options");

            _storage = storage;
            _options = options;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");

            FetchedJob fetchedJob = null;
            MySqlConnection connection = null;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                connection = _storage.CreateAndOpenConnection();
                
                try
                {
                    using (ResourceLock.AcquireOne(
                        connection, _options.TablesPrefix, LockableResource.Queue))
                    {
                        string token = Guid.NewGuid().ToString();

                        int nUpdated = connection.Execute(
                            $"update `{_options.TablesPrefix}JobQueue` set FetchedAt = UTC_TIMESTAMP(), FetchToken = @fetchToken " +
                            "where (FetchedAt is null or FetchedAt < DATE_ADD(UTC_TIMESTAMP(), INTERVAL @timeout SECOND)) " +
                            "   and Queue in @queues " +
                            "LIMIT 1;",
                            new
                            {
                                queues = queues,
                                timeout = _options.InvisibilityTimeout.Negate().TotalSeconds,
                                fetchToken = token
                            });

                        if(nUpdated != 0)
                        {
                            fetchedJob =
                                connection
                                    .Query<FetchedJob>(
                                        "select Id, JobId, Queue " +
                                        $"from `{_options.TablesPrefix}JobQueue` " +
                                        "where FetchToken = @fetchToken;",
                                        new
                                        {
                                            fetchToken = token
                                        })
                                    .SingleOrDefault();
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    Logger.ErrorException(ex.Message, ex);
                    _storage.ReleaseConnection(connection);
                    throw;
                }

                if (fetchedJob == null)
                {
                    _storage.ReleaseConnection(connection);

                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (fetchedJob == null);

            return new MySqlFetchedJob(_storage, connection, fetchedJob, _options);
        }

        public void Enqueue(IDbConnection connection, string queue, string jobId)
        {
            Enqueue(connection, null, queue, jobId);
        }

        public void Enqueue(IDbTransaction transaction, string queue, string jobId)
        {
            Enqueue(transaction.Connection, transaction, queue, jobId);
        }

        public void Enqueue(
            IDbConnection connection, IDbTransaction transaction, string queue, string jobId)
        {
            Logger.TraceFormat("Enqueue JobId={0} Queue={1}", jobId, queue);
            using (ResourceLock.AcquireOne(
                connection, transaction, _options.TablesPrefix, LockableResource.Queue))
            {
                connection.Execute(
                    $"insert into `{_options.TablesPrefix}JobQueue` (JobId, Queue) values (@jobId, @queue)",
                    new { jobId, queue },
                    transaction);
            }
        }
    }
}