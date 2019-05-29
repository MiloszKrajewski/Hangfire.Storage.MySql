using System;

namespace Hangfire.Storage.MySql.Entities
{
    internal class Server
    {
        public string Id { get; set; }
        public string Data { get; set; }
        public DateTime LastHeartbeat { get; set; }
    }
}