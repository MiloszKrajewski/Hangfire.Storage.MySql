using System;
using System.Data;
using System.Linq;
using System.Threading;
using Dapper;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql.Locking
{
	public class _ResourceLock: IDisposable
	{
		private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);

		private readonly IDbConnection _connection;
		private readonly IDbTransaction _transaction;
		private readonly string _resource;

		private _ResourceLock(
			IDbConnection connection,
			IDbTransaction transaction,
			DateTime timeout, CancellationToken token,
			string resourceName)
		{
			_connection = connection;
			_transaction = transaction;
			_resource = resourceName;
			Acquire(token, timeout);
		}

		private static DateTime Now => DateTime.UtcNow;

		private void Acquire(CancellationToken token, DateTime expiration)
		{
			// always acquire if it is free, regardless of expiration
			if (TryAcquireLock(TimeSpan.Zero))
				return;

			while (true)
			{
				var now = Now;
				if (now > expiration)
					throw new TimeoutException("Lock acquisition period expired");

				token.ThrowIfCancellationRequested();

				// trim time to be between 0s and 1s (to allow Cancellation)
				var secondsLeft = Math.Min(Math.Max(expiration.Subtract(now).TotalSeconds, 0), 1);
				if (TryAcquireLock(TimeSpan.FromSeconds(secondsLeft)))
					return;
			}
		}

		private bool TryAcquireLock(TimeSpan timeout)
		{
			var success = _connection.QueryFirst<int>(
				"select get_lock(@name, @timeout)",
				new { name = _resource, timeout = timeout.TotalSeconds },
				_transaction);
			return success != 0;
		}

		private void Release()
		{
			// Logger.TraceFormat("Release resource={0}", _resource);
			_connection.Execute("do release_lock(@name)", new { name = _resource }, _transaction);
		}

		public void Dispose() { Release(); }

		public static void ReleaseAll(IDbConnection connection) =>
			connection.Execute("do release_all_locks()");

		public static IDisposable AcquireOne(
			IDbTransaction transaction, string tablePrefix, LockableResource resource) =>
			AcquireOne(transaction.Connection, transaction, tablePrefix, resource);

		public static IDisposable AcquireOne(
			IDbConnection connection, string tablePrefix, LockableResource resource) =>
			AcquireOne(connection, null, tablePrefix, resource);

		public static IDisposable AcquireOne(
			IDbConnection connection, IDbTransaction transaction, string tablePrefix,
			LockableResource resource) =>
			AcquireOne(
				connection, transaction, tablePrefix,
				DefaultTimeout, CancellationToken.None,
				resource);

		public static IDisposable AcquireOne(
			IDbConnection connection, string tablePrefix,
			TimeSpan timeout, CancellationToken token,
			LockableResource resource) =>
			AcquireOne(connection, null, tablePrefix, timeout, token, resource);

		public static IDisposable AcquireOne(
			IDbTransaction transaction, string tablePrefix,
			TimeSpan timeout, CancellationToken token,
			LockableResource resource) =>
			AcquireOne(transaction.Connection, transaction, tablePrefix, timeout, token, resource);

		public static IDisposable AcquireOne(
			IDbConnection connection, IDbTransaction transaction, string tablePrefix,
			TimeSpan timeout, CancellationToken token,
			LockableResource resource) =>
			AcquireOne(
				connection, transaction, tablePrefix, timeout, token, resource.ToString());

		public static IDisposable AcquireMany(
			IDbConnection connection, string tablePrefix,
			TimeSpan timeout, CancellationToken token,
			LockableResource[] resources) =>
			AcquireMany(
				connection, null, tablePrefix,
				timeout, token,
				resources);

		public static IDisposable AcquireMany(
			IDbTransaction transaction, string tablePrefix,
			TimeSpan timeout, CancellationToken token,
			LockableResource[] resources) =>
			AcquireMany(
				transaction.Connection, transaction, tablePrefix,
				timeout, token,
				resources);

		public static IDisposable AcquireMany(
			IDbConnection connection, IDbTransaction transaction, string tablePrefix,
			TimeSpan timeout, CancellationToken token,
			LockableResource[] resources) =>
			AcquireMany(
				connection, transaction, tablePrefix, timeout, token,
				resources.Select(x => x.ToString()).ToArray());

		private static IDisposable AcquireOne(
			IDbConnection connection, IDbTransaction transaction, string tablePrefix,
			TimeSpan timeout, CancellationToken token,
			string resourceName) =>
			// this is slightly ineffective to use AcquireMany here
			// but it is nothing comparing to DB operation anyway 
			AcquireMany(
				connection, transaction, tablePrefix, timeout, token, resourceName);

		private static IDisposable AcquireMany(
			IDbConnection connection, IDbTransaction transaction, string tablePrefix,
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
						$"{tablePrefix}/{resourceName}");
					handles.Add(handle);

					token.ThrowIfCancellationRequested();
				}
			}
			catch
			{
				handles.Dispose();
				throw;
			}

			return handles;
		}

		public static IDisposable AcquireAny(
			IDbConnection connection, string tablePrefix, TimeSpan timeout, string resource) =>
			AcquireOne(connection, null, tablePrefix, timeout, CancellationToken.None, resource);
	}
}
