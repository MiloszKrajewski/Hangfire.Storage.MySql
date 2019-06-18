using System.Data;

namespace Hangfire.Storage.MySql.Locking
{
	public interface IContext
	{
		IDbConnection C { get; }
		IDbTransaction T { get; }
		string Prefix { get; }
	}
}
