defmodule TdCache.DomainCacheTest do
  use ExUnit.Case
  alias TdCache.DomainCache
  doctest TdCache.DomainCache

  setup do
    parent = %{id: :rand.uniform(100_000_000), name: "parent"}
    domain = %{id: :rand.uniform(100_000_000), name: "child", parent_ids: [parent.id]}

    on_exit(fn ->
      DomainCache.delete(domain.id)
      DomainCache.delete(parent.id)
    end)

    {:ok, domain: domain, parent: parent}
  end

  describe "DomainCache" do
    test "writes a domain entry in redis and reads it back", context do
      domain = context[:domain]
      {:ok, ["OK", 1, 1, 0]} = DomainCache.put(domain)
      {:ok, d} = DomainCache.get(domain.id)
      assert not is_nil(d)
      assert d.id == domain.id
      assert d.name == domain.name
      assert d.parent_ids == Enum.join(domain.parent_ids, ",")
    end

    test "deletes an entry in redis", context do
      domain = context[:domain]
      {:ok, _} = DomainCache.put(domain)
      {:ok, [1, 1, 1, 0, 0]} = DomainCache.delete(domain.id)
      assert {:ok, nil} == DomainCache.get(domain.id)
    end
  end
end
