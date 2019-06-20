using System;

namespace Hangfire.Storage.MySql.JobQueue
{
	internal class MySqlJobQueueProvider: IPersistentJobQueueProvider
	{
		private readonly IPersistentJobQueue _jobQueue;
		private readonly IPersistentJobQueueMonitoringApi _monitoringApi;

		public MySqlJobQueueProvider(MySqlStorage storage, MySqlStorageOptions options)
		{
			if (storage == null) throw new ArgumentNullException(nameof(storage));
			if (options == null) throw new ArgumentNullException(nameof(options));

			_jobQueue = new MySqlJobQueue(storage, options);
			_monitoringApi = new MySqlJobQueueMonitoringApi(storage, options);
		}

		public IPersistentJobQueue GetJobQueue() => _jobQueue;

		public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi() => _monitoringApi;
	}
}
