yredis
======

Yuilop eredis wrapper for pool and sharding.

The system can be launched throught `yredis:start_link(Config)` and Config is defined as:

```erlang
{Shard, {Server, Port, Database, Password, PoolSize}}
```

The options mean:

* *Shard*: key to use a database pool instead of other. _default_ are used if none matchs.
* *Server*: redis server IP or hostname.
* *Port*: redis server port number.
* *Database*: database number (see [Redis SELECT](http://redis.io/commands/select)).
* *Password*: password to access to redis server (an empty list means none password).
* *PoolSize*: connections pool size.

An Example:

```erlang
Config = [
   {default, {"127.0.0.1", 6379, 0, "", 5}},
   {users, {"127.0.0.1", 6380, 0, "", 5}}
].
```

You can use this configuration with the following code:

```erlang
yredis:start(Config).
yredis:command(users, ["GET", "mykey"]). %% query to users
yredis:command(customers, ["GET", "mykey"]). %% query to default
```

Thanks for using it.

