using System.Collections.Generic;

namespace Hangfire.Storage.MySql.JobQueue
{
    internal interface IPersistentJobQueueMonitoringApi
    {
        IEnumerable<string> GetQueues();
        IEnumerable<int> GetEnqueuedJobIds(string queue, int offset, int length);
        IEnumerable<int> GetFetchedJobIds(string queue, int from, int perPage);
        EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue);
    }
}