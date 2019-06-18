using System;
using System.Linq;

namespace Hangfire.Storage.MySql.Locking
{
	public enum LockableResource
	{
		Counter = 0x0001,
		Job = 0x0002,
		List = 0x0004,
		Set = 0x0008,
		Hash = 0x0010,
		Queue = 0x0020,
		Lock = 0x0040, 
		State = 0x0080, 
		Migration = 0x0100, 
		Server = 0x0200,
	}
}
