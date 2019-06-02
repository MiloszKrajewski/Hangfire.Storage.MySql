using System;
using System.Threading;
using Hangfire.Logging;
using MySql.Data.MySqlClient;

namespace Hangfire.Storage.MySql.Locking
{
	public static class Deadlock
	{
		public static T Retry<T>(Func<T> action, ILog log)
		{
			var retry = 0;
			var random = new Random();

			while (true)
			{
				try
				{
					var result = action();
					if (retry > 1)
						log?.Info($"Dead-lock {retry} resolved");

					return result;
				}
				catch (MySqlException e) when (e.Number == 1213 || e.Number == 1614)
				{
					if (++retry > 5) throw;

					var delay = retry * 5 + random.Next(retry * 25);
					if (retry > 1)
						log?.Warn($"Dead-lock {retry} encountered, retrying in {delay}ms");
					Thread.Sleep(delay);
				}
			}
		}

		public static void Retry(Action action, ILog log)
		{
			Retry(
				() => {
					action();
					return true;
				}, log);
		}
	}
}
