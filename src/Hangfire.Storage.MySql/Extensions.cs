using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text;

// ReSharper disable once CheckNamespace
namespace System
{
	internal static class Extensions
	{
		public static string Join(this IEnumerable<string> collection, string separator) =>
			string.Join(separator, collection);

		public static string NotBlank(this string value, string instead = null) =>
			string.IsNullOrWhiteSpace(value) ? instead : value;

		public static IEnumerable<T> EmptyIfNull<T>(this IEnumerable<T> collection) =>
			collection ?? Array.Empty<T>();

		public static T Rethrow<T>(this T exception)
			where T: Exception
		{
			ExceptionDispatchInfo.Capture(exception).Throw();
			// it never actually gets returned, but allows to do some syntactic trick sometimes
			return exception;
		}

		public static string Explain(this Exception exception)
		{
			if (exception == null) return "<null>";

			var result = new StringBuilder();
			Explain(exception, 0, result);
			return result.ToString();
		}

		internal static void Explain(
			Exception exception, int level, StringBuilder builder)
		{
			if (exception == null)
				return;

			builder.AppendFormat(
				"{0}@{1}: {2}\n{3}\n",
				exception.GetType().GetFriendlyName(),
				level,
				exception.Message,
				exception.StackTrace);

			var exceptions =
				exception is AggregateException aggregate
					? aggregate.InnerExceptions
					: Enumerable.Repeat(exception.InnerException, 1);

			foreach (var inner in exceptions)
				Explain(inner, level + 1, builder);
		}

		public static IEnumerable<Exception> Flatten(this Exception exception)
		{
			var exceptions = new Queue<Exception>();
			exceptions.Enqueue(exception);

			while (exceptions.Count > 0)
			{
				exception = exceptions.Dequeue();

				yield return exception;

				if (exception is AggregateException aggregate)
				{
					// special treatment for AggregateExceptions
					foreach (var inner in aggregate.InnerExceptions)
						exceptions.Enqueue(inner);
				}
				else
				{
					var inner = exception.InnerException;
					if (inner != null)
						exceptions.Enqueue(inner);
				}
			}
		}

		public static string GetFriendlyName(this Type type)
		{
			if (type == null) return "<null>";

			var typeName = type.Name;
			if (!type.IsGenericType)
				return typeName;

			var length = typeName.IndexOf('`');
			if (length < 0) length = typeName.Length;

			return new StringBuilder()
				.Append(typeName, 0, length)
				.Append('<')
				.Append(string.Join(",", type.GetGenericArguments().Select(GetFriendlyName)))
				.Append('>')
				.ToString();
		}

		public static string GetObjectFriendlyName(this object subject)
		{
			if (subject == null) return "<null>";

			return string.Format(
				"{0}@{1:x}",
				subject.GetType().GetFriendlyName(),
				RuntimeHelpers.GetHashCode(subject));
		}
		
		public static Func<object> ToFunc(this Action action) =>
			() => { action(); return null; };

		public static Func<T, object> ToFunc<T>(this Action<T> action) =>
			x => { action(x); return null; };
	}
}
