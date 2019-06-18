namespace Hangfire.Storage.MySql.JobQueue
{
    internal interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue();
        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi();
    }
}