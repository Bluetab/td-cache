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
      assert {:ok, [0, 4, 1, 1, 1, 1, 1]} == LinkCache.put(link)

      {:ok, events} = Stream.read(:redix, ["foo:events", "bar:events"], transform: true)
      assert Enum.count(events) == 2
      assert Enum.all?(events, &(&1.event == "add_link"))
      assert Enum.all?(events, &(&1.link == link_key))
      assert Enum.any?(events, &(&1.source == source_key and &1.stream == "foo:events"))
      assert Enum.any?(events, &(&1.target == target_key and &1.stream == "bar:events"))

      {:ok, l} = LinkCache.get(link.id)
      assert l.source == "#{link.source_type}:#{link.source_id}"
      assert l.target == "#{link.target_type}:#{link.target_id}"
      assert l.origin == "#{link.origin}"
      assert l.updated_at == to_string(link.updated_at)
      assert l.tags == []
    end

    test "writes a link entry with tags in redis and reads it back", context do
      link = context[:tagged_link]
      assert {:ok, [0, 4, 1, 1, 1, 1, 1, 3]} == LinkCache.put(link)

      {:ok, l} = LinkCache.get(link.id)
      assert l.source == "#{link.source_type}:#{link.source_id}"
      assert l.target == "#{link.target_type}:#{link.target_id}"
      assert l.updated_at == to_string(link.updated_at)
      assert l.origin == "#{link.origin}"
      assert Enum.sort(l.tags) == Enum.sort(link.tags)
    end

    test "writes a link without origin if no origin", context do
      link = context[:link] |> Map.delete(:origin)
      assert {:ok, [0, 3, 1, 1, 1, 1, 1]} == LinkCache.put(link)

      {:ok, l} = LinkCache.get(link.id)
      assert is_nil(l.origin)
    end

    test "writes a link with nil origin", context do
      link = context[:link] |> Map.put(:origin, nil)
      assert {:ok, [0, 3, 1, 1, 1, 1, 1]} == LinkCache.put(link)

      {:ok, l} = LinkCache.get(link.id)
      assert is_nil(l.origin)
    end

    test "only rewrites a link entry if it's update timestamp has changed", context do
      link = context[:link]
      assert {:ok, [0, 4, 1, 1, 1, 1, 1]} == LinkCache.put(link)
      assert {:ok, []} == LinkCache.put(link)

      assert {:ok, [1, 4, 0, 0, 0, 0, 0]} ==
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
        target_type: "business_concept",
        origin: "test_origin"
      })

      put_link(%{
        source_id: bc2_id,
        target_id: bc3_id,
        source_type: "business_concept",
        target_type: "business_concept",
        origin: nil
      })

      Redix.command(["KEYS", "*"])

      assert {:ok, links} = LinkCache.list("business_concept", concept2.id)
      assert Enum.map(links, & &1.resource_id) ||| ["#{bc1_id}", "#{bc3_id}"]
      assert Enum.any?(links, &(&1.origin == "test_origin"))
      assert Enum.any?(links, &(&1.origin == nil))

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

  describe "put_many/2" do
    test "inserts multiple links in batch successfully" do
      links = [
        make_link(%{id: 1, source_id: 100, target_id: 200}),
        make_link(%{id: 2, source_id: 101, target_id: 201}),
        make_link(%{id: 3, source_id: 102, target_id: 202})
      ]

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
      end)

      assert {:ok, successful, []} = LinkCache.put_many(links)
      assert length(successful) == 3

      Enum.each(links, fn link ->
        {:ok, cached_link} = LinkCache.get(link.id)
        assert cached_link.id == "#{link.id}"
        assert cached_link.source == "#{link.source_type}:#{link.source_id}"
        assert cached_link.target == "#{link.target_type}:#{link.target_id}"
      end)
    end

    test "handles empty list" do
      assert {:ok, [], []} = LinkCache.put_many([])
    end

    test "respects batch_size option" do
      links =
        Enum.map(1..5, fn id ->
          make_link(%{id: id, source_id: 100 + id, target_id: 200 + id})
        end)

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
      end)

      assert {:ok, successful, []} = LinkCache.put_many(links, batch_size: 2)
      assert length(successful) == 5

      Enum.each(links, fn link ->
        {:ok, cached_link} = LinkCache.get(link.id)
        assert cached_link.id == "#{link.id}"
      end)
    end

    test "returns failed links when redis transaction fails" do
      original_redix = Application.get_env(:td_cache, :redix_module)

      original_transaction_pipeline = &Redix.transaction_pipeline/1

      failing_redix =
        defmodule MockFailingRedix do
          def transaction_pipeline(_commands) do
            {:error, :connection_error}
          end
        end

      try do
        Application.put_env(:td_cache, :redix_module, failing_redix)

        links = [
          make_link(%{id: 1, source_id: 100, target_id: 200}),
          make_link(%{id: 2, source_id: 101, target_id: 201})
        ]

        assert {:ok, [], failed} = LinkCache.put_many(links, batch_size: 2)
        assert length(failed) == 2

        Enum.each(failed, fn link ->
          assert Map.has_key?(link, :error_reason)
          assert link.error_reason == :connection_error
        end)

        Enum.each(links, fn link ->
          Application.put_env(:td_cache, :redix_module, original_redix)
          {:ok, cached_link} = LinkCache.get(link.id)
          assert is_nil(cached_link)
          Application.put_env(:td_cache, :redix_module, failing_redix)
        end)
      after
        Application.put_env(:td_cache, :redix_module, original_redix)
      end
    end

    test "handles partial failures in batch" do
      links = [
        make_link(%{id: 1, source_id: 100, target_id: 200}),
        %{id: 2, updated_at: DateTime.utc_now(), invalid: "structure"},
        make_link(%{id: 3, source_id: 102, target_id: 202})
      ]

      valid_links = [Enum.at(links, 0), Enum.at(links, 2)]

      on_exit(fn ->
        Enum.each(valid_links, fn link -> LinkCache.delete(link.id) end)
      end)

      assert {:ok, successful, failed} = LinkCache.put_many(links)

      assert length(successful) == 2
      assert length(failed) == 1

      Enum.each(valid_links, fn link ->
        {:ok, cached_link} = LinkCache.get(link.id)
        assert cached_link.id == "#{link.id}"
      end)

      failed_link = hd(failed)
      assert Map.get(failed_link, :error_reason) == :invalid_link_structure
    end

    test "skips links that are already up-to-date" do
      link1 = make_link(%{id: 1, source_id: 100, target_id: 200})
      {:ok, _} = LinkCache.put(link1)

      link1_dup = Map.put(link1, :id, 1)

      link2 = make_link(%{id: 2, source_id: 101, target_id: 201})

      links = [link1_dup, link2]

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
      end)

      assert {:ok, successful, []} = LinkCache.put_many(links)

      assert length(successful) == 2

      Enum.each(links, fn link ->
        {:ok, cached_link} = LinkCache.get(link.id)
        assert cached_link.id == "#{link.id}"
      end)
    end

    test "publishes events for successful batch inserts" do
      links = [
        make_link(%{id: 1, source_id: 100, target_id: 200}),
        make_link(%{id: 2, source_id: 101, target_id: 201})
      ]

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
        Redix.command(["DEL", "foo:events", "bar:events"])
      end)

      Stream.trim("foo:events", 0)
      Stream.trim("bar:events", 0)

      assert {:ok, _, []} = LinkCache.put_many(links, publish: true)

      {:ok, events} = Stream.read(:redix, ["foo:events", "bar:events"], transform: true)

      assert length(events) == 4

      assert Enum.all?(events, &(&1.event == "add_link"))

      Enum.each(links, fn link ->
        link_key = "link:#{link.id}"
        source_key = "#{link.source_type}:#{link.source_id}"
        target_key = "#{link.target_type}:#{link.target_id}"

        assert Enum.any?(events, &(&1.link == link_key and &1.source == source_key))
        assert Enum.any?(events, &(&1.link == link_key and &1.target == target_key))
      end)
    end

    test "does not publish events when publish: false" do
      links = [
        make_link(%{id: 1, source_id: 100, target_id: 200}),
        make_link(%{id: 2, source_id: 101, target_id: 201})
      ]

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
        Redix.command(["DEL", "foo:events", "bar:events"])
      end)

      Stream.trim("foo:events", 0)
      Stream.trim("bar:events", 0)

      assert {:ok, _, []} = LinkCache.put_many(links, publish: false)

      {:ok, events} = Stream.read(:redix, ["foo:events", "bar:events"], transform: true)
      assert events == []
    end

    test "handles links with tags in batch" do
      links = [
        make_link(%{id: 1, source_id: 100, target_id: 200, tags: ["tag1", "tag2"]}),
        make_link(%{id: 2, source_id: 101, target_id: 201, tags: ["tag3"]}),
        make_link(%{id: 3, source_id: 102, target_id: 202})
      ]

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
      end)

      assert {:ok, successful, []} = LinkCache.put_many(links)
      assert length(successful) == 3

      {:ok, link1} = LinkCache.get(1)
      assert_lists_equal(link1.tags, ["tag1", "tag2"])

      {:ok, link2} = LinkCache.get(2)
      assert link2.tags == ["tag3"]

      {:ok, link3} = LinkCache.get(3)
      assert link3.tags == []
    end

    test "handles links with origin in batch" do
      links = [
        make_link(%{id: 1, source_id: 100, target_id: 200, origin: "origin1"}),
        make_link(%{id: 2, source_id: 101, target_id: 201}),
        make_link(%{id: 3, source_id: 102, target_id: 202})
      ]

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
      end)

      assert {:ok, successful, []} = LinkCache.put_many(links)
      assert length(successful) == 3

      {:ok, link1} = LinkCache.get(1)
      assert link1.origin == "origin1"

      {:ok, link2} = LinkCache.get(2)

      assert link2.origin == "some_origin"

      {:ok, link3} = LinkCache.get(3)
      assert link3.origin == "some_origin"
    end

    test "performance comparison with individual puts" do
      num_links = 50

      links =
        Enum.map(1..num_links, fn id ->
          make_link(%{id: id, source_id: 1000 + id, target_id: 2000 + id})
        end)

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
      end)

      {_batch_time, {:ok, batch_successful, batch_failed}} =
        :timer.tc(fn -> LinkCache.put_many(links, batch_size: 25) end)

      Enum.each(links, fn link -> LinkCache.delete(link.id) end)

      :timer.tc(fn ->
        Enum.each(links, fn link -> LinkCache.put(link, publish: false) end)
      end)

      assert length(batch_successful) == num_links
      assert batch_failed == []
    end

    test "handles very large batch" do
      num_links = 150

      links =
        Enum.map(1..num_links, fn id ->
          make_link(%{id: id, source_id: 5000 + id, target_id: 6000 + id})
        end)

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
      end)

      assert {:ok, successful, []} = LinkCache.put_many(links)
      assert length(successful) == num_links

      sample_links = Enum.take_random(links, 10)

      Enum.each(sample_links, fn link ->
        {:ok, cached_link} = LinkCache.get(link.id)
        assert cached_link.id == "#{link.id}"
      end)
    end

    test "processes links in correct batches according to batch_size" do
      links =
        Enum.map(1..7, fn id ->
          make_link(%{id: id, source_id: 100 + id, target_id: 200 + id})
        end)

      on_exit(fn ->
        Enum.each(links, fn link -> LinkCache.delete(link.id) end)
      end)

      assert {:ok, successful, []} = LinkCache.put_many(links, batch_size: 3)
      assert length(successful) == 7

      Enum.each(links, fn link ->
        {:ok, cached_link} = LinkCache.get(link.id)
        assert cached_link.id == "#{link.id}"
      end)
    end

    test "handles multiple batches where one batch fails" do
      links =
        Enum.map(1..9, fn id ->
          make_link(%{id: id, source_id: 100 + id, target_id: 200 + id})
        end)

      on_exit(fn ->
        successful_ids = [1, 2, 3, 7, 8, 9]
        Enum.each(successful_ids, fn id -> LinkCache.delete(id) end)
      end)

      original_redix = Application.get_env(:td_cache, :redix_module)

      defmodule BatchFailingRedix do
        @batch_count 0

        def transaction_pipeline(commands) do
          current_count = Process.get(:batch_count, 0)
          Process.put(:batch_count, current_count + 1)

          if current_count == 1 do
            {:error, :batch_2_failure}
          else
            generate_mock_results(commands)
          end
        end

        defp generate_mock_results(commands) do
          num_results = length(commands)

          mock_results =
            Enum.map(1..num_results, fn _ ->
              case :rand.uniform(3) do
                1 -> 1
                2 -> "OK"
                3 -> {:ok, "result"}
              end
            end)

          {:ok, mock_results}
        end
      end

      try do
        Application.put_env(:td_cache, :redix_module, BatchFailingRedix)
        Process.put(:batch_count, 0)

        assert {:ok, successful, failed} = LinkCache.put_many(links, batch_size: 3)

        assert length(successful) >= 3
        assert length(failed) >= 3

        successful_ids = Enum.map(successful, & &1.id)
        failed_ids = Enum.map(failed, & &1.id)

        assert 1 in successful_ids
        assert 2 in successful_ids
        assert 3 in successful_ids

        assert 4 in failed_ids
        assert 5 in failed_ids
        assert 6 in failed_ids

        Enum.each(failed, fn link ->
          assert Map.get(link, :error_reason) == :batch_2_failure
        end)
      after
        Application.put_env(:td_cache, :redix_module, original_redix)
        Process.delete(:batch_count)
      end
    end

    test "continues processing after batch failure when publish is false" do
      links =
        Enum.map(1..6, fn id ->
          make_link(%{id: id, source_id: 100 + id, target_id: 200 + id})
        end)

      on_exit(fn ->
        successful_ids = [1, 2, 3, 4, 5, 6]
        Enum.each(successful_ids, fn id -> LinkCache.delete(id) end)
      end)

      original_redix = Application.get_env(:td_cache, :redix_module)

      defmodule ConditionalFailingRedix do
        def transaction_pipeline(_commands) do
          current_count = Process.get(:batch_count, 0)
          Process.put(:batch_count, current_count + 1)

          case current_count do
            1 ->
              {:error, :conditional_failure}

            _ ->
              {:ok, [1, 1, 1, 1, 1, 1]}
          end
        end
      end

      try do
        Application.put_env(:td_cache, :redix_module, ConditionalFailingRedix)
        Process.put(:batch_count, 0)

        assert {:ok, successful, failed} =
                 LinkCache.put_many(links, batch_size: 2, publish: false)

        assert length(successful) == 4

        assert length(failed) == 2

        successful_ids = Enum.map(successful, & &1.id)
        failed_ids = Enum.map(failed, & &1.id)

        assert 1 in successful_ids
        assert 2 in successful_ids
        assert 3 in failed_ids
        assert 4 in failed_ids
        assert 5 in successful_ids
        assert 6 in successful_ids
      after
        Application.put_env(:td_cache, :redix_module, original_redix)
        Process.delete(:batch_count)
      end
    end

    test "handles mix of valid, invalid, and already-cached links across batches" do
      cached_link = make_link(%{id: 1, source_id: 101, target_id: 201})
      {:ok, _} = LinkCache.put(cached_link, publish: false)

      valid_links = [
        make_link(%{id: 2, source_id: 102, target_id: 202}),
        make_link(%{id: 3, source_id: 103, target_id: 203}),
        make_link(%{id: 5, source_id: 105, target_id: 205}),
        make_link(%{id: 6, source_id: 106, target_id: 206}),
        make_link(%{id: 8, source_id: 108, target_id: 208}),
        make_link(%{id: 9, source_id: 109, target_id: 209})
      ]

      invalid_links = [
        %{id: 4, updated_at: DateTime.utc_now(), invalid: "structure"},
        %{id: 7, updated_at: DateTime.utc_now(), also_invalid: true}
      ]

      all_links = [
        Enum.at(valid_links, 0),
        Enum.at(valid_links, 1),
        Enum.at(invalid_links, 0),
        cached_link,
        Enum.at(valid_links, 2),
        Enum.at(valid_links, 3),
        Enum.at(invalid_links, 1),
        Enum.at(valid_links, 4),
        Enum.at(valid_links, 5)
      ]

      on_exit(fn ->
        all_ids = 1..9 |> Enum.to_list()
        Enum.each(all_ids, fn id -> LinkCache.delete(id) end)
      end)

      assert {:ok, successful, failed} = LinkCache.put_many(all_links, batch_size: 3)

      assert length(successful) == 7
      assert length(failed) == 2

      successful_ids = Enum.map(successful, & &1.id)
      failed_ids = Enum.map(failed, & &1.id)

      assert 1 in successful_ids
      assert 2 in successful_ids
      assert 3 in successful_ids
      assert 5 in successful_ids
      assert 6 in successful_ids
      assert 8 in successful_ids
      assert 9 in successful_ids

      assert 4 in failed_ids
      assert 7 in failed_ids

      Enum.each(failed, fn link ->
        assert Map.get(link, :error_reason) == :invalid_link_structure
      end)

      Enum.each([2, 3, 5, 6, 8, 9], fn id ->
        {:ok, cached_link} = LinkCache.get(id)
        assert cached_link.id == "#{id}"
      end)

      {:ok, link1} = LinkCache.get(1)
      assert link1.id == "1"
    end
  end

  defp make_link(params \\ %{}) do
    %{
      id: System.unique_integer([:positive]),
      source_id: System.unique_integer([:positive]),
      target_id: System.unique_integer([:positive]),
      updated_at: DateTime.utc_now(),
      source_type: Map.get(params, :source_type, "foo"),
      target_type: Map.get(params, :target_type, "bar"),
      origin: "some_origin"
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
