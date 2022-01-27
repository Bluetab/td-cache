defmodule TdCache.DomainCacheTest do
  use ExUnit.Case

  import TdCache.Factory

  alias TdCache.DomainCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream

  doctest TdCache.DomainCache

  setup do
    parent = build(:domain)
    domain = build(:domain, parent_ids: [parent.id], descendent_ids: [1, 2])
    parent = Map.put(parent, :descendent_ids, [domain.id])

    on_exit(fn ->
      DomainCache.delete(domain.id)
      DomainCache.delete(parent.id)
      Redix.del!(["domain:*", "domains:*"])
    end)

    [domain: domain, parent: parent]
  end

  describe "DomainCache" do
    test "writes a domain entry in redis and reads it back", %{
      domain: %{id: id} = domain,
      parent: %{id: parent_id}
    } do
      {:ok, [5, 1, 1, 1, 1, 0, 0]} = DomainCache.put(domain)
      {:ok, d} = DomainCache.get(id)
      assert d
      assert d.id == id
      assert d.name == domain.name
      assert d.parent_ids == "#{parent_id}"
      assert d.descendent_ids == "1,2"
    end

    test "publishes an event when a domain is created", %{domain: domain} do
      {:ok, _} = DomainCache.put(domain)
      assert {:ok, events} = Stream.read(:redix, ["domain:events"], transform: true)
      assert [%{event: "domain_created"}] = events
    end

    test "does not publish an event if publish: false is specified", %{domain: domain} do
      {:ok, _} = DomainCache.put(domain, publish: false)
      assert {:ok, []} = Stream.read(:redix, ["domain:events"], transform: true)
    end

    test "updates a domain entry only if changed", %{domain: domain} do
      assert {:ok, [5, 1, 1, 1, 1, 0, 0]} = DomainCache.put(domain)
      assert {:ok, []} = DomainCache.put(domain)

      updated = %{domain | name: "updated name", updated_at: DateTime.utc_now()}

      assert {:ok, [0, 0, 0, 0, 0, 0, 0]} = DomainCache.put(updated)
      assert {:ok, %{name: "updated name"}} = DomainCache.get(domain.id)

      assert {:ok, events} = Stream.read(:redix, ["domain:events"], transform: true)
      assert [%{event: "domain_created"}, %{event: "domain_updated"}] = events
    end

    test "deletes an entry in redis", %{domain: %{id: id} = domain} do
      {:ok, _} = DomainCache.put(domain)
      {:ok, [1, 1, 1, 1, 1, 0, 1]} = DomainCache.delete(id)
      assert {:ok, nil} == DomainCache.get(id)
    end

    test "keeps a set of deleted domain ids", %{domain: %{id: id} = domain} do
      {:ok, _} = DomainCache.put(domain)
      {:ok, deleted_ids} = DomainCache.deleted_domains()
      refute Enum.member?(deleted_ids, id)
      {:ok, _} = DomainCache.delete(id)
      {:ok, deleted_ids} = DomainCache.deleted_domains()
      assert Enum.member?(deleted_ids, id)
    end
  end

  describe "external_id_to_id/1" do
    test "returns the id for a given external_id", %{
      domain: %{id: id, external_id: external_id} = domain
    } do
      {:ok, _} = DomainCache.put(domain)
      assert {:ok, ^id} = DomainCache.external_id_to_id(external_id)
    end

    test "returns error if no matching domain exists" do
      assert DomainCache.external_id_to_id("foobarbaz") == :error
    end
  end

  describe "id_to_parent_ids_map/0" do
    test "returns a map with domain_id keys and parent_ids values", %{
      domain: %{id: domain_id, parent_ids: parent_ids} = domain
    } do
      assert DomainCache.id_to_parent_ids_map() == {:ok, %{}}

      DomainCache.put(domain)

      assert DomainCache.id_to_parent_ids_map() == {:ok, %{domain_id => [domain_id | parent_ids]}}
    end
  end
end
