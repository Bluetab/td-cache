# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for
# 3rd-party users, it should be done in your "mix.exs" file.

# You can configure your application as:
#
#     config :td_perms, key: :value
#
# and access this configuration in your application as:
#
#     Application.get_env(:td_perms, :key)
#
# You can also configure a 3rd-party app:
#
#     config :logger, level: :info
#
config :td_cache, redis_host: "redis"

config :td_cache, :cache_cleaner,
  clean_on_startup: false,
  patterns: [
    "TD_CACHE_TEST:DELETE:*",
    "TD_CACHE_TEST:REMOVE:*"
  ]

config :td_cache, :concept_event_stream,
  name: :concept_event_stream,
  stream: "concept:events",
  group: "test_consumer_group",
  consumer: "consumer1"

config :td_cache, :field_event_stream,
  name: :field_event_stream,
  stream: "field:events",
  group: "test_consumer_group",
  consumer: "consumer1"

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
#     import_config "#{Mix.env}.exs"
