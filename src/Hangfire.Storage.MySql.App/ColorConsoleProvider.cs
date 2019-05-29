using System;
using Microsoft.Extensions.Logging;

namespace Hangfire.Storage.MySql.App
{
	internal class ColorConsoleProvider: ILoggerProvider, ILogger
	{
		public ILogger CreateLogger(string categoryName) => this;

		public void Log<TState>(
			LogLevel logLevel, EventId eventId, TState state, Exception exception,
			Func<TState, Exception, string> formatter)
		{
			try
			{
				Log(logLevel, formatter(state, exception));
				if (exception is null) return;

				Log(logLevel, $"{exception.GetType().Name}: {exception.Message}");
				if (string.IsNullOrWhiteSpace(exception.StackTrace)) return;

				Log(logLevel, exception.StackTrace);
			}
			catch (Exception e)
			{
				Log(LogLevel.Warning, $"<internal logging error: {e.GetType().Name}>");
			}
		}

		private static void Log(LogLevel logLevel, string message)
		{
			if (string.IsNullOrWhiteSpace(message))
				return;

			lock (Console.Out)
			{
				var color = Console.ForegroundColor;
				Console.ForegroundColor = ToColor(logLevel);
				Console.WriteLine(message);
				Console.ForegroundColor = color;
			}
		}

		private static ConsoleColor ToColor(LogLevel logLevel)
		{
			switch (logLevel)
			{
				case LogLevel.Debug: return ConsoleColor.Gray;
				case LogLevel.Information: return ConsoleColor.Cyan;
				case LogLevel.Warning: return ConsoleColor.Yellow;
				case LogLevel.Error: return ConsoleColor.Red;
				case LogLevel.Critical: return ConsoleColor.Magenta;
			}

			return ConsoleColor.DarkGray;
		}

		public bool IsEnabled(LogLevel logLevel) => true;

		public IDisposable BeginScope<TState>(TState state) => null;

		public void Dispose() { }
	}
}
