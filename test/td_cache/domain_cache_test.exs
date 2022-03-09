defmodule TdCache.DomainCacheTest do
  use ExUnit.Case

  import Assertions
  import TdCache.Factory

  alias TdCache.DomainCache
  alias TdCache.Redix

  doctest TdCache.DomainCache

  setup do
    parent = build(:domain)
    domain = build(:domain, parent_id: parent.id)

    on_exit(fn ->
      DomainCache.delete(domain.id)
      DomainCache.delete(parent.id)
      Redix.del!(["domain:*", "domains:*"])
    end)

    [domain: domain, parent: parent]
  end

  describe "DomainCache" do
    test "writes a domain entry in redis and reads it back", %{domain: %{id: id} = domain} do
      {:ok, [3, 1, 1, 1, 1, 0]} = DomainCache.put(domain)
      {:ok, d} = DomainCache.get(id)
      assert_structs_equal(d, domain, [:id, :name, :external_id])
    end

    test "updates a domain entry only if changed", %{domain: domain} do
      assert {:ok, [3, 1, 1, 1, 1, 0]} = DomainCache.put(domain)
      assert {:ok, []} = DomainCache.put(domain)

      updated = %{domain | name: "updated name", updated_at: DateTime.utc_now()}

      assert {:ok, [0, 0, 0, 0, 0, 0]} = DomainCache.put(updated)
      assert {:ok, %{name: "updated name"}} = DomainCache.get(domain.id)
    end

    test "deletes an entry in redis", %{domain: %{id: id} = domain} do
      {:ok, _} = DomainCache.put(domain)
      {:ok, [1, 1, 1, 1, 1, 1]} = DomainCache.delete(id)
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

  describe "put/1" do
    test "returns error if id is 0 or nil", %{domain: domain} do
      assert {:error, :invalid} = DomainCache.put(%{domain | id: 0})
      assert {:error, :invalid} = DomainCache.put(%{domain | id: nil})
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

  describe "count!/0" do
    test "returns the count of domains" do
      assert DomainCache.count!() == 0
      [d1, d2, d3] = build_many(3)
      DomainCache.put(d1)
      assert DomainCache.count!() == 1
      DomainCache.put(d2)
      assert DomainCache.count!() == 2
      DomainCache.put(d3)
      assert DomainCache.count!() == 3
      DomainCache.delete(d2.id)
      assert DomainCache.count!() == 2
    end
  end

  describe "tree/1" do
    setup do
      parents = build_many(3)
      domains = Enum.flat_map(parents, fn %{id: id} -> build_many(3, parent_id: id) end)
      children = Enum.flat_map(domains, fn %{id: id} -> build_many(3, parent_id: id) end)

      for domain <- children ++ domains ++ parents do
        DomainCache.put(domain, publish: false)
      end

      [parents: parents, domains: domains, children: children]
    end

    test "returns a graph of domain_ids with a fake root 0", %{
      parents: parents,
      domains: domains,
      children: children
    } do
      assert %Graph{} = tree = DomainCache.tree()
      ids = Graph.out_neighbours(tree, 0)
      assert_lists_equal(ids, parents, &(&1 == &2.id))
      ids = Enum.flat_map(ids, &Graph.out_neighbours(tree, &1))
      assert_lists_equal(ids, domains, &(&1 == &2.id))
      ids = Enum.flat_map(ids, &Graph.out_neighbours(tree, &1))
      assert_lists_equal(ids, children, &(&1 == &2.id))
      ids = Enum.flat_map(ids, &Graph.out_neighbours(tree, &1))
      assert ids == []
    end
  end

  defp build_many(count, opts \\ []) do
    Enum.map(1..count, fn _ -> build(:domain, opts) end)
  end
end
