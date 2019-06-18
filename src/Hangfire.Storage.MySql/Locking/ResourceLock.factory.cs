using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using Dapper;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql.Locking
{
	public partial class _ResourceLock
	{
		public static bool TestMany(
			IDbConnection connection, IDbTransaction transaction, string tablePrefix,
			string[] resourceNames)
		{
			var locks = resourceNames
				.Select((n, i) => $"is_used_lock(@name{i}) as lock{i}")
				.Join(", ");
			var sql = $"select connection_id() as connectionId, {locks}";
			var args = new DynamicParameters();
			foreach(var (n, i) in resourceNames.Select((n, i) => (n, i))) 
				args.Add($"name{i}", $"{tablePrefix}/{n}");
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
					var handle = new _ResourceLock(
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
