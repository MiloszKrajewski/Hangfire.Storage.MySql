using System;

namespace Hangfire.Storage.MySql
{
    public class MySqlDistributedLockException : Exception
    {
        public MySqlDistributedLockException(string message) : base(message)
        {
        }
    }
}
