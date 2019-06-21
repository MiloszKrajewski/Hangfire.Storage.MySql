using System;
using Dapper;

namespace Hangfire.Storage.MySql.Locking
{
	public static class RepeaterExtensions
	{
		public static void ExecuteOne(this IRepeaterExec repeater, Action<IContext> action) =>
			repeater.ExecuteOne(action.ToFunc());

		public static int ExecuteOne(
			this IRepeaterExec repeater, string sql, object arguments = null) =>
			repeater.ExecuteOne(x => x.C.Execute(sql.Replace("{prefix}", x.P), arguments, x.T));

		public static void ExecuteMany(this IRepeaterExec repeater, Action<IContext> action) =>
			repeater.ExecuteMany(action.ToFunc());

		public static int ExecuteMany(
			this IRepeaterExec repeater, string sql, object arguments = null) =>
			repeater.ExecuteMany(x => x.C.Execute(sql.Replace("{prefix}", x.P), arguments, x.T));
	}
}
