defmodule TdCache.LinkCacheTest do
  use ExUnit.Case
  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream
  doctest TdCache.LinkCache

  setup do
    link = make_link()

    tagged_link =
      make_link()
      |> Map.merge(Map.take(link, [:source_type, :source_id]))
      |> Map.put(:tags, ["foo", "bar", "baz"])

    on_exit(fn ->
      LinkCache.delete(link.id)
      LinkCache.delete(tagged_link.id)
      Redix.command(["DEL", "foo:events", "bar:events", "_:foo:#{link.source_id}:links"])
    end)

    {:ok, link: link, tagged_link: tagged_link}
  end

  describe "LinkCache" do
    test "writes a link entry in redis, emits events, and reads it back", context do
      link = context[:link]
      link_key = "link:#{link.id}"
      source_key = "#{link.source_type}:#{link.source_id}"
      target_key = "#{link.target_type}:#{link.target_id}"
      assert {:ok, [0, "OK", 1, 1, 1, 1, 1]} == LinkCache.put(link)

      {:ok, events} = Stream.read(:redix, ["foo:events", "bar:events"], transform: true)
      assert Enum.count(events) == 2
      assert Enum.all?(events, &(&1.event == "add_link"))
      assert Enum.all?(events, &(&1.link == link_key))
      assert Enum.any?(events, &(&1.source == source_key and &1.stream == "foo:events"))
      assert Enum.any?(events, &(&1.target == target_key and &1.stream == "bar:events"))

      {:ok, l} = LinkCache.get(link.id)
      assert l.source == "#{link.source_type}:#{link.source_id}"
      assert l.target == "#{link.target_type}:#{link.target_id}"
      assert l.updated_at == to_string(link.updated_at)
      assert l.tags == []
    end

    test "writes a link entry with tags in redis and reads it back", context do
      link = context[:tagged_link]
      assert {:ok, [0, "OK", 1, 1, 1, 1, 1, 3]} == LinkCache.put(link)

      {:ok, l} = LinkCache.get(link.id)
      assert l.source == "#{link.source_type}:#{link.source_id}"
      assert l.target == "#{link.target_type}:#{link.target_id}"
      assert l.updated_at == to_string(link.updated_at)
      assert Enum.sort(l.tags) == Enum.sort(link.tags)
    end

    test "only rewrites a link entry if it's update timestamp has changed", context do
      link = context[:link]
      assert {:ok, [0, "OK", 1, 1, 1, 1, 1]} == LinkCache.put(link)
      assert {:ok, []} == LinkCache.put(link)

      assert {:ok, [1, "OK", 0, 0, 0, 0, 0]} ==
               LinkCache.put(Map.put(link, :updated_at, DateTime.utc_now()))
    end

    test "deletes an entry in redis", context do
      link = context[:link]
      link_key = "link:#{link.id}"
      source_key = "#{link.source_type}:#{link.source_id}"
      target_key = "#{link.target_type}:#{link.target_id}"

      {:ok, _} = LinkCache.put(link)
      assert {:ok, 1} == Stream.trim("foo:events", 0)
      assert {:ok, 1} == Stream.trim("bar:events", 0)

      {:ok, [1, 1, 1, 1, 1, 1]} = LinkCache.delete(link.id)
      assert {:ok, nil} == LinkCache.get(link.id)

      {:ok, events} = Stream.read(:redix, ["foo:events", "bar:events"], transform: true)
      assert Enum.count(events) == 2
      assert Enum.all?(events, &(&1.event == "remove_link"))
      assert Enum.all?(events, &(&1.link == link_key))
      assert Enum.any?(events, &(&1.source == source_key and &1.stream == "foo:events"))
      assert Enum.any?(events, &(&1.target == target_key and &1.stream == "bar:events"))
    end

    test "returns the link count of the source and target", context do
      link = context[:link]
      source_key = "#{link.source_type}:#{link.source_id}"
      target_key = "#{link.target_type}:#{link.target_id}"

      {:ok, _} = LinkCache.put(link)
      assert {:ok, 1} == LinkCache.count(source_key, link.target_type)
      assert {:ok, 1} == LinkCache.count(target_key, link.source_type)
      {:ok, _} = LinkCache.delete(link.id)
      assert {:ok, 0} == LinkCache.count(source_key, link.target_type)
      assert {:ok, 0} == LinkCache.count(target_key, link.source_type)
    end

    test "deletes all links of a given resource", context do
      link1 = context[:link]
      link2 = context[:tagged_link]

      source_key = "#{link1.source_type}:#{link1.source_id}"
      target_key1 = "#{link1.target_type}:#{link1.target_id}"
      target_key2 = "#{link2.target_type}:#{link2.target_id}"

      {:ok, _} = LinkCache.put(link1)
      {:ok, _} = LinkCache.put(link2)
      assert {:ok, 2} == LinkCache.count(source_key, link1.target_type)
      assert {:ok, 2} == Stream.trim("bar:events", 0)
      {:ok, 2, 10} = LinkCache.delete_resource_links(link1.source_type, link1.source_id)
      assert {:ok, nil} == LinkCache.get(link1.id)
      assert {:ok, nil} == LinkCache.get(link2.id)
      assert {:ok, 0} == LinkCache.count(source_key, link1.target_type)
      assert {:ok, 0} == LinkCache.count(target_key1, link1.source_type)
      assert {:ok, 0} == LinkCache.count(target_key2, link2.source_type)

      {:ok, events} = Stream.read(:redix, "bar:events", transform: true)
      assert Enum.all?(events, &(&1.event == "remove_link"))
      assert Enum.all?(events, &(&1.stream == "bar:events"))
      assert Enum.all?(events, &(&1.source == source_key))
      assert Enum.any?(events, &(&1.link == "link:#{link1.id}"))
      assert Enum.any?(events, &(&1.link == "link:#{link2.id}"))
      assert Enum.any?(events, &(&1.target == target_key1))
      assert Enum.any?(events, &(&1.target == target_key2))
    end
  end

  defp make_link(source_type \\ "foo", target_type \\ "bar") do
    %{
      id: random_id(),
      source_id: random_id(),
      target_id: random_id(),
      updated_at: DateTime.utc_now(),
      source_type: source_type,
      target_type: target_type
    }
  end

  defp random_id, do: :rand.uniform(100_000_000)
end
