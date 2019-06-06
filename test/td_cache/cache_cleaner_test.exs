defmodule TdCache.CacheCleanerTest do
  use ExUnit.Case
  alias TdCache.CacheCleaner
  doctest TdCache.CacheCleaner

  describe "CacheCleaner" do
    test "starts automatically" do
      assert Process.whereis(CacheCleaner)
    end

    test "deletes keys from redis" do
      {:ok, _} = Redix.command(:redix, ["SET", "TD_CACHE_TEST:DELETE:STRING", "Some string"])
      {:ok, _} = Redix.command(:redix, ["SADD", "TD_CACHE_TEST:DELETE:SET", "foo1", "foo2", "foo3"])
      {:ok, _} = Redix.command(:redix, ["HMSET", "TD_CACHE_TEST:DELETE:HASH", "foo1", "bar1", "foo2", "bar2", "foo3", "bar3"])
      {:ok, _} = Redix.command(:redix, ["SET", "TD_CACHE_TEST:REMOVE:STRING", "Some string"])
      {:ok, _} = Redix.command(:redix, ["SADD", "TD_CACHE_TEST:REMOVE:SET", "foo1", "foo2", "foo3"])
      {:ok, _} = Redix.command(:redix, ["HMSET", "TD_CACHE_TEST:REMOVE:HASH", "foo1", "bar1", "foo2", "bar2", "foo3", "bar3"])
      {:ok, _} = Redix.command(:redix, ["SET", "TD_CACHE_TEST:IGNORE:STRING", "foo"])
      {:ok, _} = Redix.command(:redix, ["SET", "TD_CACHE_TEST:RETAIN:STRING", "foo"])

      {:ok, keys} = Redix.command(:redix, ["KEYS", "TD_CACHE_TEST:*"])

      assert Enum.count(keys) == 8

      CacheCleaner.clean
      # Wait for clean to complete
      CacheCleaner.ping

      {:ok, keys} = Redix.command(:redix, ["KEYS", "TD_CACHE_TEST:*"])

      assert Enum.count(keys) == 2
    end
  end

end
