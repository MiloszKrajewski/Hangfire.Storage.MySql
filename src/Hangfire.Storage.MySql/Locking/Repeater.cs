using System;
using System.Data;
using System.Diagnostics;
using Hangfire.Logging;
using System.Linq;
using System.Threading;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql.Locking
{
	public interface IRepeaterLock
	{
		IRepeaterCancel Lock(params LockableResource[] resources);
	}

	public interface IRepeaterCancel
	{
		IRepeaterExec Wait();
		IRepeaterExec Wait(DateTime dateTime, CancellationToken? token = null);
		IRepeaterExec Wait(TimeSpan timeSpan, CancellationToken? token = null);
		IRepeaterExec Wait(CancellationToken token);
	}

	public interface IRepeaterExec
	{
		IRepeaterExec Log(ILog logger, string name = null);

		T ExecuteOne<T>(Func<IContext, T> action);

		T ExecuteMany<T>(Func<IContext, T> action);
	}

	public class Repeater: IRepeaterLock, IRepeaterCancel, IRepeaterExec, IContext
	{
		public static readonly TimeSpan Quick = TimeSpan.FromSeconds(5);
		public static readonly TimeSpan Long = TimeSpan.FromSeconds(15);
		private static readonly int DeadlockThreshold = 3;

		private readonly IDbConnection _connection;
		private readonly string _prefix;

		private string _name;
		private DateTime _deadline;
		private CancellationToken _token;
		private ILog _logger;
		private string[] _resources;

		private Repeater(IDbConnection connection, string prefix)
		{
			_connection = connection;
			_prefix = prefix;
			_deadline = DateTime.UtcNow.Add(Quick);
		}

		public static IRepeaterLock Create(IDbConnection connection, string prefix) =>
			new Repeater(connection, prefix);

		private static string GetCallerName(int level = 0)
		{
			var caller = new StackFrame(2 + level).GetMethod();
			var name = $"{caller.DeclaringType.GetFriendlyName()}.{caller.Name}";
			return name;
		}

		public IRepeaterExec Log(ILog logger, string name = null)
		{
			_name = name ?? GetCallerName();
			_logger = logger;
			return this;
		}

		public IRepeaterExec Wait(DateTime dateTime, CancellationToken? token = null)
		{
			_deadline = dateTime;
			_token = token ?? CancellationToken.None;
			return this;
		}

		public IRepeaterExec Wait(TimeSpan timeSpan, CancellationToken? token = null) =>
			Wait(DateTime.UtcNow.Add(timeSpan), token);

		public IRepeaterExec Wait(CancellationToken token) =>
			Wait(Quick, token);

		public IRepeaterExec Wait() =>
			Wait(Quick);

		public IRepeaterCancel Lock(params LockableResource[] resources)
		{
			_resources = resources.Distinct().Select(x => x.ToString()).ToArray();
			return this;
		}

		public IRepeaterExec Name(string name)
		{
			_name = name;
			return this;
		}

		private T Execute<T>(bool batch, Func<IContext, T> action)
		{
			var total = 0;
			var anyLocks = _resources.Any();

			bool IsFree() =>
				!anyLocks || ResourceLock.TestMany(
					_connection, null, _prefix, _resources);

			T Loop(int retries) => RetryLoop(batch, retries, action, ref total);

			IDisposable Acquire() =>
				!anyLocks ? null : ResourceLock.AcquireMany(
					_connection, null, _prefix,
					_deadline.Subtract(DateTime.UtcNow), _token,
					_resources);

			try
			{
				return Loop(0);
			}
			catch (TimeoutException)
			{
				// ignore and retry with locks
			}

			if (IsFree())
			{
				try
				{
					return Loop(3);
				}
				catch (TimeoutException)
				{
					// ignore and retry with locks
				}
			}

			using (Acquire())
			{
				return Loop(int.MaxValue);
			}
		}

		public T ExecuteOne<T>(Func<IContext, T> action) =>
			Execute(false, action);

		public T ExecuteMany<T>(Func<IContext, T> action) =>
			Execute(true, action);

		private T RetryLoop<T>(bool batch, int retries, Func<IContext, T> action, ref int total)
		{
			bool IsDeadlock(MySqlException e) => e.Number == 1213 || e.Number == 1614;

			var attempt = 0;
			var random = new Random();

			while (true)
			{
				try
				{
					_token.ThrowIfCancellationRequested();

					var transaction = batch ? _connection.BeginTransaction() : null;
					using (transaction)
					{
						var context = Context.Create(this, transaction);
						var result = action(context);
						transaction?.Commit();

						if (total >= DeadlockThreshold)
							_logger?.Info($"Dead-lock {total} in {_name} resolved");

						return result;
					}
				}
				catch (MySqlException e) when (IsDeadlock(e))
				{
					attempt++;
					total++;

					if (total >= DeadlockThreshold)
						_logger?.Warn($"Dead-lock {total} in {_name} encountered");

					if (DateTime.UtcNow > _deadline || attempt >= retries)
						throw new TimeoutException(
							"Operation failed to finish in predefined time", e);

					_token.ThrowIfCancellationRequested();
					Thread.Sleep(random.Next(100));
				}
			}
		}

		IDbConnection IContext.C => _connection;
		IDbTransaction IContext.T => null;
		string IContext.P => _prefix;

		private class Context: IContext
		{
			private readonly IContext _context;
			private readonly IDbTransaction _transaction;

			private Context(IContext context, IDbTransaction transaction)
			{
				_context = context;
				_transaction = transaction;
			}

			public IDbConnection C => _context.C;

			public IDbTransaction T => _transaction;

			public string P => _context.P;

			public static IContext Create(Repeater repeater, IDbTransaction transaction) =>
				transaction is null ? (IContext) repeater : new Context(repeater, transaction);
		}
	}
}
