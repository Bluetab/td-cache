# TdCache

TrueDat Cache application. Provides modules for managing cached data in Redis.

## Configuration

```elixir
# In your config/config.exs file
config :td_cache, redis_host: "localhost"

# In your application.ex
children = [
  # ...
  worker(TdCache.CacheCleaner, [config])
]
```

### CacheCleaner configuration

The `TdCache.CacheCleaner` config expect a keyword list with the options:

- `clean_on_startup` - if `true`, cleanup will be performed on application startup
- `patterns` - a list of [KEYS patterns](https://redis.io/commands/keys) to be deleted from Redis

After startup, cleanup can be perfomed programatically by using `TdCache.CacheCleaner.clean`.

## Running the tests

The test environment expect a clean Redis server to be listening on port 6380.
To start redis as a docker container, run `docker-compose up -d redis`.

Run all aplication tests with `mix test`.
