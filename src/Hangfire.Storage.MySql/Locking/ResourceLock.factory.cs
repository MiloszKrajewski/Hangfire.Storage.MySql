using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using Dapper;
using System.Linq;
using System.Threading;

namespace Hangfire.Storage.MySql.Locking
{
	public partial class ResourceLock
	{
		public static bool TestMany(
			IDbConnection connection, IDbTransaction transaction, string tablePrefix,
			string[] resourceNames)
		{
			KeyValuePair<string, object> Pair(string n, int i) =>
				new KeyValuePair<string, object>($"name{i}", $"{tablePrefix}/{n}");

			var sql = BuildTestQuery(resourceNames.Length);
			var args = new DynamicParameters(resourceNames.Select(Pair));
			var results = connection.QueryFirst(sql, args, transaction)
				as IDictionary<string, object>;
			var connectionId = Convert.ToInt32(results?["connectionId"]);
			return results
				.EmptyIfNull()
				.Where(kv => kv.Key.StartsWith("lock"))
				.Select(kv => kv.Value)
				.Select(v => v is null || v is DBNull ? -1 : Convert.ToInt32(v))
				.All(v => v == -1 || v == connectionId);
		}
		
		private static readonly ConcurrentDictionary<int, string> TestQueries = 
			new ConcurrentDictionary<int, string>();

		private static string BuildTestQuery(int resources) =>
			TestQueries.GetOrAdd(
				resources, r => {
					var locks = Enumerable
						.Range(0, resources)
						.Select(i => $"is_used_lock(@name{i}) as lock{i}")
						.Join(", ");
					var sql = $"select connection_id() as connectionId, {locks}";
					return sql;
				});

		public static IDisposable AcquireMany(
			IDbConnection connection, IDbTransaction transaction, string prefix,
			TimeSpan timeout, CancellationToken token,
			params string[] resourceNames)
		{
			var handles = new DisposableBag();

			try
			{
				var expiration = Now.Add(timeout); // stop trying @

				// order alphabetically to prevent dead-locks
				var orderedResourceNames = resourceNames.OrderBy(x => x);

				foreach (var resourceName in orderedResourceNames)
				{
					var handle = new ResourceLock(
						connection, transaction,
						expiration, token,
						$"{prefix}/{resourceName}");
					handles.Add(handle);
				}
			}
			catch
			{
				handles.Dispose();
				throw;
			}

			return handles;
		}

		private static string[] ResourceNames(IEnumerable<LockableResource> resources) =>
			resources.Select(x => x.ToString()).ToArray();

		public static IDisposable AcquireAny(
			IDbConnection connection, string prefix,
			TimeSpan timeout, CancellationToken token, string resource) =>
			AcquireMany(connection, null, prefix, timeout, token, resource);

		public static IDisposable AcquireMany(
			IDbConnection connection, string prefix,
			TimeSpan timeout, params LockableResource[] resources) =>
			AcquireMany(
				connection, null, prefix,
				timeout, CancellationToken.None,
				ResourceNames(resources));

		public static IDisposable AcquireAny(
			IDbConnection connection, string prefix, TimeSpan timeout, string resource) =>
			AcquireMany(
				connection, null, prefix,
				timeout, CancellationToken.None,
				resource);
	}
}
