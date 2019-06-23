using System;
using System.Data;
using System.Data.Common;
using Dapper;
using System.Linq;
using System.Threading;

namespace Hangfire.Storage.MySql.Locking
{
	public partial class ConnectionLock: IDisposable
	{
		private readonly IDbConnection _connection;
		private readonly IDbTransaction _transaction;
		private readonly string _resource;
		private bool _disposed;

		private ConnectionLock(
			IDbConnection connection, IDbTransaction transaction,
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

		private bool TryAcquireLock(TimeSpan timeout) =>
			TryAcquireLock(_connection, _transaction, timeout, _resource);

		private void Release() => ReleaseOne(_connection, _transaction, _resource);

		public void Dispose()
		{
			if (_disposed)
				return;

			_disposed = true;
			Release();
		}

		public static void ReleaseAll(IDbConnection connection, DbTransaction transaction) =>
			connection.Execute("do release_all_locks()", null, transaction);

		private static void ReleaseOne(
			IDbConnection connection, IDbTransaction transaction, string name) =>
			connection.Execute("do release_lock(@name)", new { name }, transaction);

		private static bool TryAcquireLock(
			IDbConnection connection, IDbTransaction transaction, TimeSpan timeout, string name)
		{
			var success = connection.QueryFirst<int>(
				"select get_lock(@name, @timeout)",
				new { name, timeout = timeout.TotalSeconds },
				transaction);
			return success != 0;
		}
	}
}
