defmodule TdCache.DomainCacheTest do
  use ExUnit.Case

  alias TdCache.DomainCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream

  doctest TdCache.DomainCache

  setup do
    parent = %{
      id: System.unique_integer([:positive]),
      external_id: "parent",
      name: "parent",
      updated_at: DateTime.utc_now()
    }

    domain = %{
      id: System.unique_integer([:positive]),
      external_id: "domain",
      name: "child",
      parent_ids: [parent.id],
      updated_at: DateTime.utc_now()
    }

    on_exit(fn ->
      DomainCache.delete(domain.id)
      DomainCache.delete(parent.id)
      Redix.del!("domain:*")
    end)

    {:ok, domain: domain, parent: parent}
  end

  describe "DomainCache" do
    test "writes a domain entry in redis and reads it back", %{domain: %{id: id} = domain} do
      {:ok, ["OK", 1, 1, 1, 0, 0]} = DomainCache.put(domain)
      {:ok, d} = DomainCache.get(id)
      assert not is_nil(d)
      assert d.id == id
      assert d.name == domain.name
      assert d.parent_ids == Enum.join(domain.parent_ids, ",")
      assert {:ok, events} = Stream.read(:redix, ["domain:events"], transform: true)
      assert [%{event: "domain_created"}] = events
    end

    test "updates a domain entry only if changed", %{domain: domain} do
      assert {:ok, ["OK", 1, 1, 1, 0, 0]} = DomainCache.put(domain)
      assert {:ok, []} = DomainCache.put(domain)

      updated = %{domain | name: "updated name", updated_at: DateTime.utc_now()}

      assert {:ok, ["OK", 0, 0, 0, 0, 0]} = DomainCache.put(updated)
      assert {:ok, %{name: "updated name"}} = DomainCache.get(domain.id)

      assert {:ok, events} = Stream.read(:redix, ["domain:events"], transform: true)
      assert [%{event: "domain_created"}, %{event: "domain_updated"}] = events
    end

    test "deletes an entry in redis", %{domain: %{id: id} = domain} do
      {:ok, _} = DomainCache.put(domain)
      {:ok, [1, 1, 1, 1, 0, 1]} = DomainCache.delete(id)
      assert {:ok, nil} == DomainCache.get(id)
    end

    test "get deleted domain ids", %{domain: %{id: id} = domain} do
      {:ok, _} = DomainCache.put(domain)
      assert {:ok, []} = DomainCache.deleted_domains()
      {:ok, [1, 1, 1, 1, 0, 1]} = DomainCache.delete(id)
      assert {:ok, [^id]} = DomainCache.deleted_domains()
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
end
