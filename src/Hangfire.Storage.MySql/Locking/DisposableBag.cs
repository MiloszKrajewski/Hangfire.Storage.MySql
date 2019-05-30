using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

// ReSharper disable once CheckNamespace
namespace System
{
	/// <summary>A disposable collection of disposables.</summary>
	/// <seealso cref="IDisposable" />
	public class DisposableBag: IDisposable
	{
		private readonly object _lock = new object();
		private readonly List<IDisposable> _bag = new List<IDisposable>();
		private bool _disposed;

		/// <summary>Creates disposable collection of disposables.</summary>
		/// <returns>New disposable collection of disposables</returns>
		public static DisposableBag Create() => new DisposableBag();

		/// <summary>Creates disposable collection of disposables starting with provided ones.</summary>
		/// <param name="disposables">The disposables.</param>
		/// <returns>New disposable collection of disposables.</returns>
		public static DisposableBag Create(IEnumerable<IDisposable> disposables) =>
			new DisposableBag(disposables);

		/// <summary>Creates disposable collection of disposables.</summary>
		/// <param name="disposables">The disposables.</param>
		/// <returns>New disposable collection of disposables.</returns>
		public static DisposableBag Create(params IDisposable[] disposables) =>
			new DisposableBag(disposables);

		/// <summary>Initializes a new instance of the <see cref="DisposableBag"/> class.</summary>
		public DisposableBag() { }

		/// <summary>Initializes a new instance of the <see cref="DisposableBag"/> class.</summary>
		/// <param name="disposables">The disposables.</param>
		public DisposableBag(IEnumerable<IDisposable> disposables)
		{
			_bag.AddRange(disposables); // safe in constructor
		}

		/// <summary>Adds many disposables.</summary>
		/// <param name="disposables">The disposables.</param>
		public void AddMany(IEnumerable<IDisposable> disposables)
		{
			lock (_lock)
			{
				if (_disposed)
				{
					DisposeMany(disposables.ToArray());
				}
				else
				{
					_bag.AddRange(disposables);
				}
			}
		}

		/// <summary>Disposes items.</summary>
		/// <param name="disposables">The disposables.</param>
		/// <exception cref="AggregateException">Combines exceptions thrown when disposing items.</exception>
		private static void DisposeMany(IEnumerable<IDisposable> disposables)
		{
			IList<Exception> exceptions = null;
			foreach (var disposable in disposables)
			{
				try
				{
					disposable.Dispose();
				}
				catch (Exception e)
				{
					(exceptions = exceptions ?? new List<Exception>()).Add(e);
				}
			}

			if (exceptions != null)
				throw new AggregateException(exceptions);
		}

		/// <summary>Adds disposable.</summary>
		/// <typeparam name="T">Type of object.</typeparam>
		/// <param name="item">The disposable item.</param>
		/// <returns>Same item for further processing.</returns>
		[SuppressMessage("ReSharper", "ExpressionIsAlwaysNull")]
		public T Add<T>(T item) where T: IDisposable
		{
			if (ReferenceEquals(item, null))
				return item; // item is 'null' but 'null' does not work with generic T

			lock (_lock)
			{
				if (_disposed)
				{
					item.Dispose();
				}
				else
				{
					_bag.Add(item);
				}

				return item;
			}
		}

		/// <summary>Performs application-defined tasks associated with 
		/// freeing, releasing, or resetting unmanaged resources.</summary>
		public void Dispose()
		{
			IDisposable[] disposables;
			lock (_lock)
			{
				_disposed = true;
				disposables = _bag.ToArray();
				_bag.Clear();
			}

			DisposeMany(disposables);
		}
	}
}
