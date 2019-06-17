defmodule TdCache.LinkCacheTest do
  use ExUnit.Case
  alias TdCache.LinkCache
  alias TdCache.Redix, as: Redis
  doctest TdCache.LinkCache

  setup do
    link = %{
      id: :rand.uniform(100_000_000),
      source_id: :rand.uniform(100_000_000),
      target_id: :rand.uniform(100_000_000),
      updated_at: DateTime.utc_now(),
      source_type: "foo",
      target_type: "bar"
    }

    on_exit(fn ->
      LinkCache.delete(link.id)
      Redis.command(["DEL", "foo:events", "bar:events"])
    end)

    {:ok, link: link}
  end

  describe "LinkCache" do
    test "starts automatically" do
      assert Process.whereis(LinkCache)
    end

    test "writes a link entry in redis and reads it back", context do
      link = context[:link]
      assert {:ok, [0, "OK", 1, 1, 1]} == LinkCache.put(link)
      {:ok, l} = LinkCache.get(link.id)
      assert l.source == "#{link.source_type}:#{link.source_id}"
      assert l.target == "#{link.target_type}:#{link.target_id}"
      assert l.ts == to_string(link.updated_at)
      assert l.tags == []
    end

    test "deletes an entry in redis", context do
      link = context[:link]
      {:ok, _} = LinkCache.put(link)
      {:ok, _} = LinkCache.delete(link.id)
      assert {:ok, nil} == LinkCache.get(link.id)
    end
  end
end
