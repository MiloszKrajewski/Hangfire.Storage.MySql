using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using CommandLine;
using Dapper;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;

namespace XaRecover
{
	public class Options
	{
		[Option(
			'h', "host", Default = "localhost",
			HelpText = "Database server (host[:port], default: localhost)")]
		public string Server { get; set; }

		[Option('u', "user", Default = "mysql", HelpText = "User (default: mysql)")]
		public string User { get; set; }

		[Option('p', "pass", Default = "mysql", HelpText = "Password (default: mysql)")]
		public string Password { get; set; }
	}

	[SuppressMessage("ReSharper", "InconsistentNaming")]
	[SuppressMessage("ReSharper", "IdentifierTypo")]
	public struct XaData
	{
		public int formatID;
		public int gtrid_length;
		public int bqual_length;
		public string data;
	}

	class Program
	{
		private static readonly Regex ServerRx = new Regex(@"^(?<host>.*?)(\:(?<port>\d+))?$");

		static void Main(string[] args)
		{
			try
			{
				Parser.Default
					.ParseArguments<Options>(args)
					.WithParsed(Execute)
					.WithNotParsed(PrintErrors);
			}
			catch (Exception e)
			{
				Console.WriteLine($"{e.GetType().Name}: {e.Message}\n{e.StackTrace}");
			}
		}

		private static void Execute(Options options)
		{
			var (host, port) = ParseServer(options);
			var (user, pass) = (options.User, options.Password);
			var connectionString = $"server={host};port={port};uid={user};pwd={pass}";

			using (var connection = new MySqlConnection(connectionString))
			{
				var transactions = connection.Query<XaData>("xa recover convert xid").ToArray();
				Console.WriteLine($"Found {transactions.Length} distributed transactions...");
				foreach (var transaction in transactions)
				{
					RecoverTransaction(connection, transaction);
				}
			}
		}

		private static void RecoverTransaction(MySqlConnection connection, XaData transaction)
		{
			try
			{
				var (transId, branchId) = ParseData(transaction.data, transaction.gtrid_length);
				var formatId = transaction.formatID;
				
				connection.Execute($"xa rollback 0x{transId},0x{branchId},{formatId}");
				Console.WriteLine($"Recovered transaction {transaction.data}");
			}
			catch
			{
				Console.WriteLine($"Failed to recover {transaction.data}");
			}
		}

		private static (string, string) ParseData(string transData, int transIdLength)
		{
			if (!transData.StartsWith("0x"))
				throw new ArgumentException($"{transData} does not match 0x... format");

			var length = transIdLength * 2;
			var transId = transData.Substring(2, length);
			var branchId = transData.Substring(2 + length); // to end
			return (transId, branchId);
		}

		private static (string, int) ParseServer(Options options)
		{
			var m = ServerRx.Match(options.Server);
			if (!m.Success)
				throw new ArgumentException($"{options.Server} is not valid database address");

			var host = m.Groups["host"].Value;
			var port = int.Parse(m.Groups["port"].Value.NotBlank("3306"));

			return (host, port);
		}

		private static void PrintErrors(IEnumerable<Error> enumerable)
		{
			throw new NotImplementedException();
		}
	}
}
