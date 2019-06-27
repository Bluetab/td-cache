defmodule TdCache.ConceptCacheTest do
  use ExUnit.Case
  alias TdCache.ConceptCache
  alias TdCache.DomainCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream
  doctest TdCache.ConceptCache

  @stream "business_concept:events"

  setup do
    domain = %{
      id: random_id(),
      name: "foo",
      parent_ids: [random_id(), random_id()]
    }

    concept = %{id: random_id(), business_concept_version_id: random_id(), name: "foo"}

    {:ok, _} = DomainCache.put(domain)

    Redix.command(["DEL", @stream, "business_concept:ids:active", "business_concept:ids:inactive"])

    on_exit(fn ->
      ConceptCache.delete(concept.id)
      DomainCache.delete(domain.id)

      Redix.command([
        "DEL",
        @stream,
        "business_concept:ids:active",
        "business_concept:ids:inactive"
      ])
    end)

    {:ok, concept: concept, domain: domain}
  end

  describe "ConceptCache" do
    test "starts automatically" do
      assert Process.whereis(ConceptCache)
    end

    test "writes a concept entry in redis and reads it back", context do
      concept = context[:concept]
      {:ok, ["OK", 1, 0, 1]} = ConceptCache.put(concept)
      {:ok, c} = ConceptCache.get(concept.id)
      assert not is_nil(c)
      assert c.id == concept.id
      assert c.name == concept.name
      assert c.business_concept_version_id == "#{concept.business_concept_version_id}"
      assert c.link_count == 0
      assert c.rule_count == 0
    end

    test "writes a concept entry with domain in redis and reads it back", context do
      domain = context[:domain]

      concept =
        context[:concept]
        |> Map.put(:domain_id, domain.id)

      {:ok, ["OK", 1, 0, 1]} = ConceptCache.put(concept)
      {:ok, c} = ConceptCache.get(concept.id)
      assert not is_nil(c)
      assert c.id == concept.id
      assert c.name == concept.name
      assert c.business_concept_version_id == "#{concept.business_concept_version_id}"
      assert c.link_count == 0
      assert c.rule_count == 0
      assert c.domain_id == "#{domain.id}"
      assert not is_nil(c.domain)
      assert c.domain.id == domain.id
      assert c.domain.name == domain.name
      assert c.domain.parent_ids == Enum.join(domain.parent_ids, ",")
    end

    test "reads the domain_ids property of a concept", context do
      domain = context[:domain]

      concept =
        context[:concept]
        |> Map.put(:domain_id, domain.id)

      {:ok, _} = ConceptCache.put(concept)
      {:ok, domain_ids} = ConceptCache.get(concept.id, :domain_ids)
      assert domain_ids == [domain.id] ++ domain.parent_ids
    end

    test "deletes an entry in redis", context do
      concept = context[:concept]
      {:ok, _} = ConceptCache.put(concept)
      {:ok, [1, 1, 1, 1]} = ConceptCache.delete(concept.id)
      assert {:ok, nil} == ConceptCache.get(concept.id)
    end

    test "publishes an event when an entry is removed", context do
      concept = context[:concept]
      Redix.command!(["SADD", "business_concept:ids:active", concept.id])
      {:ok, _} = ConceptCache.delete(concept.id)

      {:ok, [e]} = Stream.read([@stream], transform: true)
      assert e.event == "remove_concepts"
      assert e.ids == "#{concept.id}"
    end

    test "publishes an event when an entry is restored", context do
      concept = context[:concept]
      Redix.command!(["SADD", "business_concept:ids:inactive", concept.id])
      {:ok, _} = ConceptCache.put(concept)

      {:ok, [e]} = Stream.read([@stream], transform: true)
      assert e.event == "restore_concepts"
      assert e.ids == "#{concept.id}"
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

      {:ok, [e]} = Stream.read([@stream], transform: true)
      assert e.event == "remove_concepts"
      assert e.ids |> String.split(",") == removed_ids
      Stream.trim(@stream, 0)

      assert {:ok, [_, _, _, _, _, _, _, removed_ids, restored_ids, _]} =
               ConceptCache.put_active_ids(current_ids)

      assert MapSet.new(restored_ids) == MapSet.new(deleted_ids)
      assert MapSet.new(removed_ids) == MapSet.new(new_ids)

      {:ok, [e1, e2]} = Stream.read([@stream], transform: true)
      assert e1.event == "restore_concepts"
      assert e1.ids |> String.split(",") == restored_ids
      assert e2.event == "remove_concepts"
      assert e2.ids |> String.split(",") == removed_ids
    end
  end

  defp random_id, do: :rand.uniform(100_000_000)
end
