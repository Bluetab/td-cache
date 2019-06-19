defmodule TdCache.ConceptCacheTest do
  use ExUnit.Case
  alias TdCache.ConceptCache
  alias TdCache.DomainCache
  doctest TdCache.ConceptCache

  setup do
    domain = %{
      id: :rand.uniform(100_000_000),
      name: "foo",
      parent_ids: [:rand.uniform(100_000_000), :rand.uniform(100_000_000)]
    }

    concept = %{id: :rand.uniform(100_000_000), name: "foo"}

    {:ok, _} = DomainCache.put(domain)

    on_exit(fn ->
      ConceptCache.delete(concept.id)
      DomainCache.delete(domain.id)
    end)

    {:ok, concept: concept, domain: domain}
  end

  describe "ConceptCache" do
    test "starts automatically" do
      assert Process.whereis(ConceptCache)
    end

    test "writes a concept entry in redis and reads it back", context do
      concept = context[:concept]
      {:ok, _} = ConceptCache.put(concept)
      {:ok, c} = ConceptCache.get(concept.id)
      assert not is_nil(c)
      assert c.id == concept.id
      assert c.name == concept.name
      assert c.link_count == 0
      assert c.rule_count == 0
    end

    test "writes a concept entry with domain in redis and reads it back", context do
      domain = context[:domain]

      concept =
        context[:concept]
        |> Map.put(:domain_id, domain.id)

      {:ok, _} = ConceptCache.put(concept)
      {:ok, c} = ConceptCache.get(concept.id)
      assert not is_nil(c)
      assert c.id == concept.id
      assert c.name == concept.name
      assert c.link_count == 0
      assert c.rule_count == 0
      assert c.domain_id == "#{domain.id}"
      assert not is_nil(c.domain)
      assert c.domain.id == domain.id
      assert c.domain.name == domain.name
      assert c.domain.parent_ids == Enum.join(domain.parent_ids, ",")
    end

    test "deletes an entry in redis", context do
      concept = context[:concept]
      {:ok, _} = ConceptCache.put(concept)
      {:ok, _} = ConceptCache.delete(concept.id)
      assert {:ok, nil} == ConceptCache.get(concept.id)
    end
  end
end
