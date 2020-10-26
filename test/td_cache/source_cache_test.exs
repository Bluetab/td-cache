defmodule TdCache.SourceCacheTest do
  use ExUnit.Case
  alias TdCache.Redix
  alias TdCache.SourceCache
  doctest TdCache.SourceCache

  @ids_to_external_ids_key "sources:ids_external_ids"

  setup do
    source = %{id: :rand.uniform(100_000_000), external_id: "foo", config: %{}}

    on_exit(fn ->
      SourceCache.delete(source.id)
      Redix.command(["DEL", @ids_to_external_ids_key])
    end)

    {:ok, source: source}
  end

  describe "SourceCache" do
    test "writes a source entry in redis and reads it back", context do
      source = context[:source]
      {:ok, _} = SourceCache.put(source)
      {:ok, s} = SourceCache.get(source.id)
      assert s == source
    end

    test "deletes an entry in redis", context do
      source = context[:source]
      {:ok, _} = SourceCache.put(source)
      {:ok, _} = SourceCache.delete(source.id)
      assert {:ok, nil} == SourceCache.get(source.id)
    end

    test "get all sources entries in redis", context do
      source = context[:source]
      {:ok, _} = SourceCache.put(source)
      assert {:ok, keys} = SourceCache.sources()
      assert keys == [source.id]
    end
  end
end
