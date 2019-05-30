using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Hangfire.Storage.MySql.Test")]

namespace Hangfire.Storage.MySql
{
	public class AssemblyHook
	{
		private AssemblyHook() { }
	}
}
