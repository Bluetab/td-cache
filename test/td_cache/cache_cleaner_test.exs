defmodule TdCache.CacheCleanerTest do
  use ExUnit.Case

  alias TdCache.CacheCleaner
  alias TdCache.Redix

  setup_all do
    on_exit(fn -> Redix.del!("TD_CACHE_TEST:*") end)
  end

  describe "CacheCleaner" do
    test "deletes keys from redis" do
      commands = [
        ["SET", "TD_CACHE_TEST:DELETE:STRING", "Some string"],
        ["SADD", "TD_CACHE_TEST:DELETE:SET", "foo1", "foo2", "foo3"],
        ["HSET", "TD_CACHE_TEST:DELETE:HASH", "foo1", "bar1", "foo2", "bar2", "foo3", "bar3"],
        ["SET", "TD_CACHE_TEST:REMOVE:STRING", "Some string"],
        ["SADD", "TD_CACHE_TEST:REMOVE:SET", "foo1", "foo2", "foo3"],
        ["HSET", "TD_CACHE_TEST:REMOVE:HASH", "foo1", "bar1", "foo2", "bar2", "foo3", "bar3"],
        ["SET", "TD_CACHE_TEST:IGNORE:STRING", "foo"],
        ["SET", "TD_CACHE_TEST:RETAIN:STRING", "foo"]
      ]

      {:ok, _} = Redix.transaction_pipeline(commands)

      {:ok, keys} = Redix.command(["KEYS", "TD_CACHE_TEST:*"])

      assert Enum.count(keys) == 8

      :ok =
        CacheCleaner.clean([
          "TD_CACHE_TEST:DELETE:*",
          "TD_CACHE_TEST:REMOVE:*"
        ])

      {:ok, keys} = Redix.command(["KEYS", "TD_CACHE_TEST:*"])

      assert Enum.count(keys) == 2
    end
  end
end
