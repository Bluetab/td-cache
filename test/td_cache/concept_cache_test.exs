defmodule TdCache.ConceptCacheTest do
  use ExUnit.Case

  import TdCache.Factory

  alias TdCache.CacheHelpers
  alias TdCache.ConceptCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream

  doctest TdCache.ConceptCache

  @stream "business_concept:events"

  setup do
    root = build(:domain)
    parent = build(:domain, parent_id: root.id)
    domain = build(:domain, parent_id: parent.id, parent_ids: [parent.id, root.id])
    shared_to = build(:domain, parent_id: parent.id)
    concept = build(:concept)

    Enum.each([root, parent, domain, shared_to], &CacheHelpers.put_domain/1)

    Redix.command([
      "DEL",
      @stream,
      "business_concept:ids:active",
      "business_concept:ids:inactive"
    ])

    on_exit(fn ->
      ConceptCache.delete(concept.id)

      Redix.command([
        "DEL",
        @stream,
        "business_concept:ids:active",
        "business_concept:ids:inactive",
        "business_concept:ids:confidential",
        "domain:deleted_ids"
      ])
    end)

    [concept: concept, domain: domain, shared_to: shared_to]
  end

  describe "ConceptCache" do
    test "starts automatically" do
      assert Process.whereis(ConceptCache)
    end

    test "writes a concept entry in redis and reads it back", context do
      concept = context[:concept]
      {:ok, [5, 1, 1, 1, 0, 1, 0]} = ConceptCache.put(concept)
      {:ok, c} = ConceptCache.get(concept.id)
      assert c
      assert c.id == concept.id
      assert c.name == concept.name
      assert c.business_concept_version_id == "#{concept.business_concept_version_id}"
      assert c.type == concept.type
      assert c.link_count == 0
      assert c.rule_count == 0
      assert c.concept_count == 0
      assert c.shared_to_ids == []
      assert c.status == "#{concept.status}"
    end

    test "writes a concept entry with domain in redis and reads it back", %{
      domain: domain,
      concept: concept
    } do
      concept = Map.put(concept, :domain_id, domain.id)

      {:ok, [6, 1, 1, 1, 0, 1, 0]} = ConceptCache.put(concept)
      {:ok, c} = ConceptCache.get(concept.id)
      assert c
      assert c.id == concept.id
      assert c.name == concept.name
      assert c.business_concept_version_id == "#{concept.business_concept_version_id}"
      assert c.link_count == 0
      assert c.link_tags == []
      assert c.rule_count == 0
      assert c.concept_count == 0
      assert c.domain_id == domain.id
      assert c.domain
      assert c.domain.id == domain.id
      assert c.domain.name == domain.name
      assert c.domain.parent_id == domain.parent_id
      assert c.status == "#{concept.status}"
    end

    test "reads the content property of a concept", %{concept: %{content: content} = concept} do
      {:ok, _} = ConceptCache.put(concept)
      assert {:ok, ^content} = ConceptCache.get(concept.id, :content)
    end

    test "reads many concepts", %{domain: %{id: domain_id}} do
      %{id: shared_domain_id} = build(:domain)

      [%{id: id1}, %{id: id2}, %{id: id3}] =
        inserted_concepts =
        Enum.map(1..3, fn i ->
          concept =
            if rem(i, 2) == 0 do
              build(:concept, domain_id: domain_id, shared_to_ids: [shared_domain_id])
            else
              build(:concept, domain_id: domain_id)
            end

          {:ok, _} = ConceptCache.put(concept)
          concept
        end)

      ids = [id1, id2, id3]

      not_valid_id = Enum.max(ids) + 1

      {:ok, cache_concepts} = ConceptCache.get_many(Enum.shuffle(ids ++ [not_valid_id]))

      assert Enum.count(cache_concepts) == 3

      assert Enum.all?(cache_concepts, fn %{id: concept_id, name: concept_name} ->
               Enum.find(inserted_concepts, fn %{id: inserted_id, name: inserted_name} ->
                 inserted_id == concept_id and inserted_name == concept_name
               end)
             end)

      Redix.command!(["DEL"] ++ Redix.command!(["KEYS", "business_concept:*"]))
      Redix.command!(["DEL", "domain:deleted_ids"])
    end

    test "reads the content property of a concept with specific lang", %{
      concept: concept
    } do
      es_name = "concept_name_es"
      es_value = ["xyz", "qux"]

      i18n = %{
        "es" => %{
          "name" => es_name,
          "content" => %{
            "foo" => es_value
          }
        }
      }

      {:ok, _} = ConceptCache.put(Map.put(concept, :i18n, i18n))

      assert {:ok, %{name: ^es_name, content: %{"foo" => ^es_value}}} =
               ConceptCache.get(concept.id, lang: "es")
    end

    test "reads the i18n property of a concept", %{
      concept: concept
    } do
      es_name = "concept_name_es"
      es_value = ["xyz", "qux"]

      i18n = %{
        "es" => %{
          "name" => es_name,
          "content" => %{
            "foo" => es_value
          }
        }
      }

      {:ok, _} = ConceptCache.put(Map.put(concept, :i18n, i18n))

      assert {:ok, %{"es" => %{"name" => ^es_name, "content" => %{"foo" => ^es_value}}}} =
               ConceptCache.get_i18n(concept.id)
    end

    test "reads the domain_ids property of a concept", %{domain: domain, concept: concept} do
      concept = Map.put(concept, :domain_id, domain.id)

      {:ok, _} = ConceptCache.put(concept)
      {:ok, domain_ids} = ConceptCache.get(concept.id, :domain_ids)
      assert domain_ids == [domain.id | domain.parent_ids]
    end

    test "deletes an entry in redis", context do
      concept = context[:concept]
      {:ok, _} = ConceptCache.put(concept)
      {:ok, [1, 1, 1, 1, 0]} = ConceptCache.delete(concept.id)
      assert {:ok, nil} == ConceptCache.get(concept.id)
    end

    test "publishes an event when an entry is removed", context do
      concept = context[:concept]
      Redix.command!(["SADD", "business_concept:ids:active", concept.id])
      {:ok, _} = ConceptCache.delete(concept.id)

      {:ok, [e]} = Stream.read(:redix, [@stream], transform: true)
      assert e.event == "remove_concepts"
      assert e.ids == "#{concept.id}"
    end

    test "doesn't publish an event when an entry is removed if publish is false", context do
      concept = context[:concept]
      Redix.command!(["SADD", "business_concept:ids:active", concept.id])
      {:ok, _} = ConceptCache.delete(concept.id, publish: false)
      assert {:ok, []} = Stream.read(:redix, [@stream], transform: true)
    end

    test "publishes an event when an entry is restored", context do
      concept = context[:concept]
      Redix.command!(["SADD", "business_concept:ids:inactive", concept.id])
      {:ok, _} = ConceptCache.put(concept)

      {:ok, [e]} = Stream.read(:redix, [@stream], transform: true)
      assert e.event == "restore_concepts"
      assert e.ids == "#{concept.id}"
    end

    test "doesn't publish an event when an entry is restored if publish is false", context do
      concept = context[:concept]
      Redix.command!(["SADD", "business_concept:ids:inactive", concept.id])
      {:ok, _} = ConceptCache.put(concept, publish: false)

      assert {:ok, []} = Stream.read(:redix, [@stream], transform: true)
    end

    test "updates active and inactive ids, publishes events identifying removed and restored ids" do
      ids =
        1..100
        |> Enum.map(fn _ -> random_id() end)
        |> Enum.uniq()
        |> Enum.take(50)
        |> Enum.map(&to_string/1)

      {current_ids, new_ids} = Enum.split(ids, 40)
      {deleted_ids, next_ids} = Enum.split(ids, 5)
      {:ok, _} = ConceptCache.put_active_ids(current_ids)
      Stream.trim(@stream, 0)

      assert {:ok, [_, _, _, _, _, _, _, removed_ids, restored_ids, _]} =
               ConceptCache.put_active_ids(next_ids)

      assert MapSet.new(removed_ids) == MapSet.new(deleted_ids)
      assert restored_ids == []

      {:ok, [e]} = Stream.read(:redix, [@stream], transform: true)
      assert e.event == "remove_concepts"
      assert e.ids |> String.split(",") == removed_ids
      Stream.trim(@stream, 0)

      assert {:ok, [_, _, _, _, _, _, _, removed_ids, restored_ids, _]} =
               ConceptCache.put_active_ids(current_ids)

      assert MapSet.new(restored_ids) == MapSet.new(deleted_ids)
      assert MapSet.new(removed_ids) == MapSet.new(new_ids)

      {:ok, [e1, e2]} = Stream.read(:redix, [@stream], transform: true)
      assert e1.event == "restore_concepts"
      assert e1.ids |> String.split(",") == restored_ids
      assert e2.event == "remove_concepts"
      assert e2.ids |> String.split(",") == removed_ids
    end

    test "updates confidential ids, publishes events identifying the ids" do
      ids =
        1..100
        |> Enum.map(fn _ -> random_id() end)
        |> Enum.uniq()
        |> Enum.take(50)
        |> Enum.map(&to_string/1)

      {current_ids, next_ids} = Enum.split(ids, 40)

      {:ok, _} = ConceptCache.put_confidential_ids(current_ids)
      Stream.trim(@stream, 0)

      assert {:ok, [_, 10, _]} = ConceptCache.put_confidential_ids(next_ids)

      {:ok, [e]} = Stream.read(:redix, [@stream], transform: true)
      assert e.event == "confidential_concepts"

      assert e.ids
             |> String.split(",")
             |> Enum.all?(fn ci -> Enum.any?(next_ids, &(&1 == ci)) end)
    end

    test "writes a concept with content and reads it back", context do
      concept = context[:concept]
      {:ok, [5, 1, 1, 1, 0, 1, 0]} = ConceptCache.put(concept)
      {:ok, c} = ConceptCache.get(concept.id)
      assert c
      assert c.id == concept.id
      assert c.name == concept.name
      assert c.business_concept_version_id == "#{concept.business_concept_version_id}"
      assert c.link_count == 0
      assert c.rule_count == 0
      assert c.concept_count == 0
      assert c.content == concept.content
      assert %{"data_owner" => _} = c.content
      assert c.status == "#{concept.status}"
    end
  end

  test "put confidential/public concept updates confidential ids list", context do
    # confidential concept
    %{id: id} = concept = context[:concept] |> Map.put(:confidential, true)

    assert {:ok, [5, 1, 1, 1, 0, 1, 1]} = ConceptCache.put(concept)
    assert {:ok, 1} = ConceptCache.member_confidential_ids(id)
    assert ConceptCache.confidential?(id)

    # public concept
    concept = Map.put(concept, :confidential, false)

    assert {:ok, [0, 0, 0, 0, 0, 0, 1]} = ConceptCache.put(concept)
    assert {:ok, 0} = ConceptCache.member_confidential_ids(id)
    refute ConceptCache.confidential?(id)
  end

  test "puts concept with shared domain ids", context do
    %{id: shared_id, name: name} = shared_to = context[:shared_to]
    %{id: id} = concept = Map.put(context[:concept], :shared_to_ids, [shared_to.id])

    {:ok, [5, 1, 1, 1, 0, 1, 0]} = ConceptCache.put(concept)
    {:ok, %{shared_to: [%{id: ^shared_id, name: ^name}]}} = ConceptCache.get(id)
  end

  defp random_id, do: System.unique_integer([:positive])
end
