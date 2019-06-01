using System;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// ReSharper disable UnusedParameter.Local

namespace Hangfire.Storage.MySql.App
{
	internal static class Program
	{
		private static readonly Subject<Unit> Ticks = new Subject<Unit>();

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
			Ticks
				.Buffer(TimeSpan.FromSeconds(5))
				.Select(l => l.Count)
				.Subscribe(c => Console.WriteLine($"{c / 5.0:N}/s"));

			var connectionString = "Server=localhost;Database=hangfire;Uid=scim;Pwd=sc1m;";
			var tablePrefix = "lib1_";

			GlobalConfiguration.Configuration.UseLogProvider(new HLogProvider(loggerFactory));

			using (var storage = new MySqlStorage(
				connectionString, new MySqlStorageOptions { TablesPrefix = tablePrefix }))
			{
				var cancel = new CancellationTokenSource();
				var task = Task.WhenAll(
					// Task.Run(() => Producer(loggerFactory, storage, cancel.Token), cancel.Token),
					Task.Run(() => Consumer(loggerFactory, storage, cancel.Token), cancel.Token),
					Task.CompletedTask
				);

				Console.ReadLine();
				cancel.Cancel();
				task.Wait(CancellationToken.None);
			}
		}

		private static Task Producer(
			ILoggerFactory loggerFactory, JobStorage storage, CancellationToken token)
		{
			var logger = loggerFactory.CreateLogger("main");
			var counter = 0;

			void Create()
			{
				var client = new BackgroundJobClient(storage);
				while (!token.IsCancellationRequested)
				{
					var i = Interlocked.Increment(ref counter);
					try
					{
						client.Schedule(() => HandleJob(i), DateTimeOffset.UtcNow);
						Ticks.OnNext(Unit.Default);
					}
					catch (Exception e)
					{
						logger.LogError(e, "Scheduling failed");
					}
				}
			}

			return Task.WhenAll(
				Task.Run(() => Create(), token),
				Task.Run(() => Create(), token),
				Task.Run(() => Create(), token),
				Task.Run(() => Create(), token));
		}

		private static Task Consumer(
			ILoggerFactory loggerFactory, JobStorage storage, CancellationToken token)
		{
			var server = new BackgroundJobServer(
				new BackgroundJobServerOptions { WorkerCount = 16 },
				storage);

			return Task.Run(
				() => {
					token.WaitHandle.WaitOne();
					server.SendStop();
					server.WaitForShutdown(TimeSpan.FromSeconds(30));
				}, token);
		}

		public static void HandleJob(int i) { Ticks.OnNext(Unit.Default); }
	}
}
