using System.Data;
using System.Threading;

namespace Hangfire.Storage.MySql.JobQueue
{
    public interface IPersistentJobQueue
    {
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
        void Enqueue(IDbConnection connection, string queue, string jobId);
        void Enqueue(IDbTransaction transaction, string queue, string jobId);
    }
}