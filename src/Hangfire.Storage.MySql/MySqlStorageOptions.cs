using System;

namespace Hangfire.Storage.MySql
{
	public class MySqlStorageOptions
	{
		public static readonly string DefaultTablesPrefix = string.Empty;
		public static readonly TimeSpan MinimumPollInterval = TimeSpan.FromSeconds(1);

		private TimeSpan _queuePollInterval;

		public MySqlStorageOptions()
		{
			TransactionIsolationLevel = System.Transactions.IsolationLevel.ReadCommitted;
			QueuePollInterval = TimeSpan.FromSeconds(15);
			JobExpirationCheckInterval = TimeSpan.FromHours(1);
			CountersAggregateInterval = TimeSpan.FromMinutes(5);
			PrepareSchemaIfNecessary = true;
			DashboardJobListLimit = 50000;
			TransactionTimeout = TimeSpan.FromMinutes(1);
			InvisibilityTimeout = TimeSpan.FromMinutes(30);

			TablesPrefix = DefaultTablesPrefix;
		}

		public System.Transactions.IsolationLevel? TransactionIsolationLevel { get; set; }

		public TimeSpan QueuePollInterval
		{
			get => _queuePollInterval;
			set => _queuePollInterval = value <= MinimumPollInterval ? MinimumPollInterval : value;
		}

		public bool PrepareSchemaIfNecessary { get; set; }
		public TimeSpan JobExpirationCheckInterval { get; set; }
		public TimeSpan CountersAggregateInterval { get; set; }

		public int? DashboardJobListLimit { get; set; }
		public TimeSpan TransactionTimeout { get; set; }

		[Obsolete("Will be removed in 2.0.0.")]
		public TimeSpan InvisibilityTimeout { get; set; }

		public string TablesPrefix { get; set; }
	}
}
