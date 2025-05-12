defmodule TdCache.LinkCacheTest do
  use ExUnit.Case

  import TdCache.Factory
  import Assertions
  import TdCache.TestOperators

  alias TdCache.ConceptCache
  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream
  alias TdCache.StructureCache

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

      Redix.command([
        "DEL",
        "foo:events",
        "bar:events",
        "_:foo:#{link.source_id}:links",
        "link:commands"
      ])
    end)

    {:ok, link: link, tagged_link: tagged_link}
  end

  describe "LinkCache" do
    test "writes a link entry in redis, emits events, and reads it back", context do
      link = context[:link]
      link_key = "link:#{link.id}"
      source_key = "#{link.source_type}:#{link.source_id}"
      target_key = "#{link.target_type}:#{link.target_id}"
      assert {:ok, [0, 3, 1, 1, 1, 1, 1]} == LinkCache.put(link)

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
      assert {:ok, [0, 3, 1, 1, 1, 1, 1, 3]} == LinkCache.put(link)

      {:ok, l} = LinkCache.get(link.id)
      assert l.source == "#{link.source_type}:#{link.source_id}"
      assert l.target == "#{link.target_type}:#{link.target_id}"
      assert l.updated_at == to_string(link.updated_at)
      assert Enum.sort(l.tags) == Enum.sort(link.tags)
    end

    test "only rewrites a link entry if it's update timestamp has changed", context do
      link = context[:link]
      assert {:ok, [0, 3, 1, 1, 1, 1, 1]} == LinkCache.put(link)
      assert {:ok, []} == LinkCache.put(link)

      assert {:ok, [1, 3, 0, 0, 0, 0, 0]} ==
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

    test "returns the tags of the source and target type" do
      assert {:ok, []} = LinkCache.tags("foo:123", "bar")
      assert {:ok, []} = LinkCache.tags("bar:456", "foo")

      put_link(%{source_id: "123", tags: ["tag1"], target_id: "456"}, publish: false)
      put_link(%{source_id: "123", tags: ["tag1", "tag2"]}, publish: false)
      put_link(%{source_id: "123"}, publish: false)
      put_link(%{target_id: "456", tags: ["tag3"]}, publish: false)

      assert {:ok, tags} = LinkCache.tags("foo:123", "bar")
      assert_lists_equal(tags, ["tag1", "tag2"])

      assert {:ok, tags} = LinkCache.tags("bar:456", "foo")
      assert_lists_equal(tags, ["tag1", "tag3"])
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

    test "lists all links", %{link: %{id: id1} = link, tagged_link: %{id: id2} = tagged_link} do
      {:ok, _} = LinkCache.put(link)
      {:ok, _} = LinkCache.put(tagged_link)
      assert [_, _] = links = LinkCache.list_links()
      assert Enum.any?(links, &(&1.id == "#{id1}"))
      assert Enum.any?(links, &(&1.id == "#{id2}"))
    end

    test "linked_source_ids returns a MapSet containing the ids of the specified resource type" do
      for target_id <- 42..45 do
        put_link(%{source_id: 123, target_id: target_id, source_type: "bar", target_type: "foo"})
      end

      put_link(%{source_type: "foo", source_id: 42, target_type: "bar", target_id: 99})
      put_link(%{source_type: "foo", source_id: 42, target_type: "baz", target_id: 99})

      assert LinkCache.linked_source_ids("xxx", "yyy") == []
      assert LinkCache.linked_source_ids("foo", "baz") == [42]
      assert LinkCache.linked_source_ids("foo", "bar") == [42, 43, 44, 45]
      assert LinkCache.linked_source_ids("bar", "foo") == [99, 123]
    end

    test "list/3 retrieves child links" do
      %{id: bc1_id} = concept1 = build(:concept)
      %{id: bc2_id} = concept2 = build(:concept)
      %{id: bc3_id} = concept3 = build(:concept)

      ConceptCache.put(concept1)
      ConceptCache.put(concept2)
      ConceptCache.put(concept3)

      put_link(%{
        source_id: bc1_id,
        target_id: bc2_id,
        source_type: "business_concept",
        target_type: "business_concept"
      })

      put_link(%{
        source_id: bc2_id,
        target_id: bc3_id,
        source_type: "business_concept",
        target_type: "business_concept"
      })

      Redix.command(["KEYS", "*"])

      assert {:ok, links} = LinkCache.list("business_concept", concept2.id)
      assert Enum.map(links, & &1.resource_id) ||| ["#{bc1_id}", "#{bc3_id}"]

      assert {:ok, [link]} =
               LinkCache.list("business_concept", concept2.id,
                 without_parent_business_concepts: true
               )

      assert link.resource_id == "#{bc3_id}"
    end

    test "returns a list of n random links" do
      %{id: bc1_id} = concept1 = build(:concept)
      %{id: bc2_id} = concept2 = build(:concept)
      %{id: bc3_id} = concept3 = build(:concept)

      structure = %{
        id: System.unique_integer([:positive]),
        name: "name",
        external_id: "ext_id",
        group: "group",
        type: "type",
        path: ["foo", "bar"],
        updated_at: DateTime.utc_now(),
        metadata: %{"alias" => "source_alias"},
        system_id: 1,
        domain_ids: [1, 2],
        deleted_at: DateTime.utc_now()
      }

      ConceptCache.put(concept1)
      ConceptCache.put(concept2)
      ConceptCache.put(concept3)

      StructureCache.put(structure)

      put_link(%{
        source_id: structure.id,
        target_id: bc1_id,
        source_type: "data_structure",
        target_type: "business_concept"
      })

      put_link(%{
        source_id: structure.id,
        target_id: bc2_id,
        source_type: "data_structure",
        target_type: "business_concept"
      })

      put_link(%{
        source_id: structure.id,
        target_id: bc3_id,
        source_type: "data_structure",
        target_type: "business_concept"
      })

      {:ok, links} =
        LinkCache.list_rand_links("data_structure", structure.id, "business_concept", 2)

      assert Enum.count(links) == 2

      {:ok, links} =
        LinkCache.list_rand_links("data_structure", structure.id, "business_concept", 3)

      assert Enum.count(links) == 3
    end
  end

  defp make_link(params \\ %{}) do
    %{
      id: System.unique_integer([:positive]),
      source_id: System.unique_integer([:positive]),
      target_id: System.unique_integer([:positive]),
      updated_at: DateTime.utc_now(),
      source_type: Map.get(params, :source_type, "foo"),
      target_type: Map.get(params, :target_type, "bar")
    }
    |> Map.merge(params)
  end

  defp put_link(params, opts \\ []) do
    link = make_link(params)
    on_exit(fn -> LinkCache.delete(link.id) end)
    LinkCache.put(link, opts)
    link
  end
end
