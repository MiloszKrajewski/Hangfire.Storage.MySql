using System;
using Hangfire.Logging;
using Microsoft.Extensions.Logging;
using LogLevel = Hangfire.Logging.LogLevel;

namespace Hangfire.Storage.MySql.App
{
	public class HLogProvider: ILogProvider
	{
		private readonly ILoggerFactory _factory;
		public HLogProvider(ILoggerFactory factory) => _factory = factory;
		public ILog GetLogger(string name) => new HLog(_factory.CreateLogger(name));

		private class HLog: ILog
		{
			private readonly ILogger _logger;
			public HLog(ILogger logger) { _logger = logger; }

			public bool Log(
				LogLevel logLevel, Func<string> messageFunc, Exception exception = null)
			{
				if (messageFunc != null && logLevel >= LogLevel.Info)
					_logger.Log(TranslateLevel(logLevel), exception, messageFunc());
				return true;
			}

			private static Microsoft.Extensions.Logging.LogLevel TranslateLevel(LogLevel logLevel)
			{
				switch (logLevel)
				{
					case LogLevel.Debug: return Microsoft.Extensions.Logging.LogLevel.Debug;
					case LogLevel.Trace: return Microsoft.Extensions.Logging.LogLevel.Trace;
					case LogLevel.Info: return Microsoft.Extensions.Logging.LogLevel.Information;
					case LogLevel.Warn: return Microsoft.Extensions.Logging.LogLevel.Warning;
					case LogLevel.Error: return Microsoft.Extensions.Logging.LogLevel.Error;
					case LogLevel.Fatal: return Microsoft.Extensions.Logging.LogLevel.Critical;
					default: return Microsoft.Extensions.Logging.LogLevel.Error;
				}
			}
		}
	}
}
