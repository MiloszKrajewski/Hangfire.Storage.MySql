using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Storage.MySql.Locking
{
	public interface ILease<out T>: IDisposable
	{
		T Subject { get; }
	}

	/// <summary>
	/// Simple Pool pattern. Provides pool of objects of the same type.
	/// It is used when object creation takes long time, otherwise don't use it.
	/// </summary>
	/// <typeparam name="T">Type objects.</typeparam>
	public class Pool<T>: IDisposable
	{
		#region fields

		/// <summary>The queue.</summary>
		private readonly ConcurrentQueue<T> _queue = new ConcurrentQueue<T>();

		/// <summary>Produce callback.</summary>
		private readonly Func<T> _produce;

		/// <summary>Recycle callback.</summary>
		private readonly Func<T, bool> _recycle;

		/// <summary>Minimum pool size.</summary>
		private readonly int _minimumSize;

		/// <summary>Maximum pool size.</summary>
		private readonly int _maximumSize;

		/// <summary>Current pool size.</summary>
		private int _currentSize;

		/// <summary>Cancellation token to stop background processing.</summary>
		private readonly CancellationTokenSource _cancel;

		/// <summary>Background process cleaning up queue.</summary>
		private readonly Task _hoover;

		#endregion

		#region constructor

		/// <summary>Initializes a new instance of the <see cref="Pool&lt;T&gt;"/> class.</summary>
		/// <param name="minimumSize">The minimum size of the pool.</param>
		/// <param name="maximumSize">The maximum size of the pool.</param>
		/// <param name="produce">The produce callback (required).</param>
		/// <param name="recycle">The recycle callback (optional).</param>
		public Pool(
			int minimumSize,
			int maximumSize,
			Func<T> produce,
			Func<T, bool> recycle = null)
		{
			_minimumSize = Math.Max(1, minimumSize);
			_maximumSize = Math.Max(_minimumSize, maximumSize);
			_currentSize = 0;
			_produce = produce ?? throw new ArgumentNullException(nameof(produce));
			_recycle = recycle;
			_cancel = new CancellationTokenSource();
			_hoover = Task.Run(() => Hoover(_cancel.Token), _cancel.Token);
		}

		#endregion

		#region public interface

		/// <summary>Executes the action using one object from pool.</summary>
		/// <param name="action">The action.</param>
		public void Use(Action<T> action)
		{
			var item = Acquire();
			try
			{
				action(item);
			}
			finally
			{
				RecycleAsync(item);
			}
		}

		/// <summary>Executes the action using object from pool.</summary>
		/// <typeparam name="R">Type of function result.</typeparam>
		/// <param name="action">The action.</param>
		/// <returns>Value returned by function.</returns>
		public R Use<R>(Func<T, R> action)
		{
			var item = Acquire();
			try
			{
				return action(item);
			}
			finally
			{
				RecycleAsync(item);
			}
		}

		/// <summary>Borrows object from pool. It will be returned when Lease is disposed.</summary>
		/// <returns>Borrowed object.</returns>
		public ILease<T> Borrow() => new Lease(this);

		public void Dispose()
		{
			_cancel.Cancel();
			_hoover.Wait();
			_queue.ToArray().ForEach(item => item.TryDispose());
		}

		#endregion

		#region implementation

		/// <summary>Acquires new object. It is taken from the pool, or if pool is empty 
		/// new one is created.</summary>
		/// <returns>Object.</returns>
		private T Acquire()
		{
			if (!_queue.TryDequeue(out var item))
			{
				item = _produce();
			}
			else
			{
				Interlocked.Decrement(ref _currentSize);
			}

			return item;
		}

		/// <summary>Recycles the specified item.</summary>
		/// <param name="item">The item.</param>
		private void RecycleAsync(T item)
		{
			if (ReferenceEquals(null, item)) return;

			Task.Factory.StartNew(() => Recycle(item)).Forget();
		}

		private void Recycle(T item)
		{
			if (_recycle == null || _recycle(item))
			{
				if (Interlocked.Increment(ref _currentSize) <= _maximumSize)
				{
					_queue.Enqueue(item);
					return;
				}

				// decrement because it was incremented above queue maximum size
				Interlocked.Decrement(ref _currentSize);
			}

			item.TryDispose();
		}

		private void TryDiscard()
		{
			if (Interlocked.Decrement(ref _currentSize) >= _minimumSize)
			{
				if (_queue.TryDequeue(out var item))
				{
					item.TryDispose();
				}
				else
				{
					Interlocked.Increment(ref _currentSize);
				}
			}
			else
			{
				// it was a "mistake" to decrement set it back to previous value
				Interlocked.Increment(ref _currentSize);
			}
		}

		private async Task Hoover(CancellationToken token)
		{
			while (!token.IsCancellationRequested)
			{
				await Task.Delay(TimeSpan.FromSeconds(1), token);
				TryDiscard();
			}
		}

		private class Lease: ILease<T>
		{
			private readonly Pool<T> _pool;
			private readonly T _item;

			public Lease(Pool<T> pool)
			{
				_pool = pool;
				_item = pool.Acquire();
			}

			public T Subject => _item;

			public void Dispose() => _pool.RecycleAsync(_item);
		}

		#endregion
	}
}
