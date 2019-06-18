using System.Data;
using System.Data.Common;
using System.Threading;
using Hangfire.Storage.MySql.Locking;

namespace Hangfire.Storage.MySql.JobQueue
{
	public interface IPersistentJobQueue
	{
		IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
		void Enqueue(IDbConnection connection, string queue, string jobId);
		void Enqueue(IContext context, string queue, string jobId);
	}
}
