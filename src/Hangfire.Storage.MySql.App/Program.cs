using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// ReSharper disable UnusedParameter.Local

namespace Hangfire.Storage.MySql.App
{
	internal static class Program
	{
		public static void Main(string[] args)
		{
			var loggerFactory = new LoggerFactory();
			loggerFactory.AddProvider(new ColorConsoleProvider());
			var serviceCollection = new ServiceCollection();
			serviceCollection.AddSingleton<ILoggerFactory>(loggerFactory);

			Configure(serviceCollection);
			var serviceProvider = serviceCollection.BuildServiceProvider();
			Execute(loggerFactory, serviceProvider, args);
		}

		private static void Configure(ServiceCollection serviceCollection) { }

		private static void Execute(
			ILoggerFactory loggerFactory, IServiceProvider serviceProvider, string[] args)
		{
			var log = loggerFactory.CreateLogger("main");

			log.LogTrace("Trace...");
			log.LogDebug("Debug...");
			log.LogInformation("Info...");
			log.LogWarning("Warning...");
			log.LogError("Error...");
			log.LogCritical("Critical...");

			Task.Run(() => Console.ReadLine()).Wait(TimeSpan.FromSeconds(5));
		}
	}
}
