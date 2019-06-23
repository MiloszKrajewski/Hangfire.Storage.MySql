using System;
using System.Data;
using Dapper;
using System.Linq;

namespace Hangfire.Storage.MySql.Locking
{
	public class DistributedLocks: IDisposable
	{
		private readonly ILease<IDbConnection> _lease;
		private readonly MySqlStorage _storage;
		private readonly MySqlStorageOptions _options;

		public DistributedLocks(MySqlStorage storage, MySqlStorageOptions options)
		{
			_storage = storage ?? throw new ArgumentNullException(nameof(storage));
			_options = options ?? throw new ArgumentNullException(nameof(options));
			_lease = _storage.BorrowConnection();
		}

		public IDisposable Acquire(string name, TimeSpan timeout)
		{
			name = $"{_options.TablesPrefix}/{name}";
			var expiration = DateTime.UtcNow.Add(timeout);
			var connection = _lease.Subject;
			return TryAcquire(connection, name) ?? AcquireLoop(name, expiration);
		}

		private static IDisposable TryAcquire(
			IDbConnection connection, string name, double timeout = 0)
		{
			lock (connection)
			{
				var success = connection.QueryFirst<int>(
					@"select get_lock(@name, @timeout)",
					new { name, timeout }) != 0;
				return success ? ReleaseLock(connection, name) : null;
			}
		}

		private static IDisposable ReleaseLock(IDbConnection connection, string name)
		{
			void Release()
			{
				lock (connection)
				{
					connection.Execute(
						@"do release_lock(@name)",
						new { name });
				}
			}

			return Disposable.Create(Release);
		}

		private IDisposable AcquireLoop(string name, DateTime expiration)
		{
			var lease = _storage.BorrowConnection();
			var connection = lease.Subject;
			try
			{
				while (true)
				{
					if (DateTime.UtcNow > expiration)
						throw new TimeoutException($"Failed to acquire '{name}' lock.");

					var disposable = TryAcquire(connection, name, 0.25);
					if (disposable != null)
						return DisposableBag.Create(disposable, lease);
				}
			}
			catch
			{
				lease.Dispose();
				throw;
			}
		}

		public void Dispose() => _lease.Dispose();
	}
}
