namespace Hangfire.Storage.MySql.JobQueue
{
	internal class EnqueuedAndFetchedCountDto
	{
		public int? EnqueuedCount { get; set; }
		public int? FetchedCount { get; set; }
	}
}
