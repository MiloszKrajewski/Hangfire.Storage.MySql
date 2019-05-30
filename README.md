# Hangfire.Storage.MySql

[![NuGet Stats](https://img.shields.io/nuget/v/Hangfire.Storage.MySql.svg)](https://www.nuget.org/packages/Hangfire.Storage.MySql)

This is MySQL storage adapter for [Hangfire](https://www.hangfire.io/).

It is based on [Hangfire.MySqlStorage](https://github.com/arnoldasgudas/Hangfire.MySqlStorage) 
and [Hangfire.MySql.Core](https://github.com/stulzq/Hangfire.MySql.Core). 

# Story

It is relatively complicated.

As far as I understand `Hangfire.MySqlStorage` was first (and is listed on Hangfire website as 3rd party driver). It
was not released for .NET Core (.NET Standard) for a very long time though so unofficial fork has been created, 
`Hangfire.MySql.Core`, which main purpose was: .NET Core. It also included few fixes but nothing revolutionary.
After a while original `Hangfire.MySqlStorage` caught up with .NET Core and some fixes (nothing revolutionary either). 

Unfortunately, there are still more serious bugs which reveal themselves under load.

If you see exceptions saying `Too many connections`, `Deadlock found when trying to get lock; try restarting transaction`,
`Transaction branch was rolled back: deadlock was detected`, or something like `Timeout expired` you are probably having 
concurrency problems related to deadlocks.

You can find some signs that authors tried to address those issues, like some 
`SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED` here and there but this is not solving problems in high-load
situations.

# Identified concurrency problems

There are actually 3 scenarios I personally encountered:
* Single statement dead-lock with exception (MySQL error #1213)
* Transaction dead-lock with exception (MySQL error #1614)
* Transaction (only?) (true) dead-lock freezing whole operation   

As first two are relatively benign (we could just catch exception and retry) the third one is deadly.
This is just a speculation, but it seems like it locks tables permanently, so even killing application is not removing 
locks from tables. This is weird and I don't understand it completely, so maybe I'm wrong about the diagnosis (but not 
about symptoms).

## Deadlocks caused by poor performance

They are not really caused by poor performance, but poor performance increases window of opportunity, so deadlock 
caused by other bugs have greater chance to manifest themselves.   

* Missing index for `Job.ExpireAt` and `Set.ExpireAt` (but also other tables) which kills `ExpirationManager` 
    (note: without index on `ExpireAt` delete operation leads to full table scan and locks all rows)
* Missing index for `Set.Score` which kills `MySqlStorageConnection.GetFirstByLowestScoreFromSet` (note: as name 
    suggests it is about lowest score, doing full table scan on with >1m rows every second has major impact)
* Missing index for `Job.FetchToken` which slows down `MySqlJobQueue.Dequeue` (note: in my case list of jobs 
    to be done NOW was quite long so this index was important)
    
**NOTE: This library modifies database slightly (adds some indexes), so after running it for the first time
you have slightly different database than original, for better and for worse.**
    
## Deadlocks caused by no preemptive locking

`MySqlWriteOnlyTransaction` seems to have an idea of locking resources modified by transactions 
(methods like: `AcquireJobLock`) but actual implementation is empty, so when transaction starts
nothing is locked, so MySQL locks whatever MySQL wants to lock, in random order leading to complete standstill 
and leaking connections.

I hoped for catching MySQL error #1614 and retrying transaction but excepion was not always thrown,
sometimes it was just freezing.

So preemptive locking was the safest but.

The problem with preemptive locking is, that if you decide to use them, you have to use them everywhere.
So, all `inserts`, `updates`, and `deletes` are now using preemptive locks on appropriate resources.

# Usage

As in original `Hangfire.MySqlStorage`:

```c#
var options = 
    new MySqlStorageOptions {
        TransactionIsolationLevel = IsolationLevel.ReadCommitted,
        QueuePollInterval = TimeSpan.FromSeconds(15),
        JobExpirationCheckInterval = TimeSpan.FromHours(1),
        CountersAggregateInterval = TimeSpan.FromMinutes(5),
        PrepareSchemaIfNecessary = true,
        DashboardJobListLimit = 50000,
        TransactionTimeout = TimeSpan.FromMinutes(1),
        TablesPrefix = "Hangfire"
    };
var storage = new MySqlStorage(connectionString, options);
```

In **ASP.NET Core** you register hangfire in services:

```c#
services.AddHangfire(config => config.UseStorage(storage)));
```

but for example, in command line application, you just create `BackgroundJobServer` youself:

```c#
var server = new BackgroundJobServer(storage);
```

there is also global configuration:

```c#
GlobalConfiguration.Configuration.UseStorage(storage);
```

There rest is in (capable) hands of Hangfire.

# Next

I tried to keep modifications as minimal as possible to make merging back to `Hangfire.MySqlStorage` 
as easy as possible, and once changes are merged I could go back to gardening.
But... It seems like all issues are not answered, several pull requests are not merged, and nobody knows
if they ever will be. The code is full of deprecation warnings and ReSharper issues so my fixes are just a beginning,
and there is a lot of work to be done, to polish it.  

If I decide to keep developing it, I guess, it might be better to fork it from official 
[Microsoft SQL Server](https://github.com/HangfireIO/Hangfire) implementation.

# Build

```shell
paket install
fake build
```
