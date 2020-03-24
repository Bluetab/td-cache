defmodule TdCache.DomainCacheTest do
  use ExUnit.Case
  alias TdCache.DomainCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream

  doctest TdCache.DomainCache

  setup do
    parent = %{id: :rand.uniform(100_000_000), name: "parent", updated_at: DateTime.utc_now()}

    domain = %{
      id: :rand.uniform(100_000_000),
      name: "child",
      parent_ids: [parent.id],
      updated_at: DateTime.utc_now()
    }

    on_exit(fn ->
      DomainCache.delete(domain.id)
      DomainCache.delete(parent.id)
      Redix.command(["DEL", "domain:events"])
    end)

    {:ok, domain: domain, parent: parent}
  end

  describe "DomainCache" do
    test "writes a domain entry in redis and reads it back", context do
      domain = context[:domain]
      {:ok, ["OK", 1, 1, 0, 0]} = DomainCache.put(domain)
      {:ok, d} = DomainCache.get(domain.id)
      assert not is_nil(d)
      assert d.id == domain.id
      assert d.name == domain.name
      assert d.parent_ids == Enum.join(domain.parent_ids, ",")
      assert {:ok, events} = Stream.read(:redix, ["domain:events"], transform: true)
      assert Enum.count(events) == 1
      assert Enum.all?(events, &(&1.event == "domain_updated"))
      assert Enum.all?(events, &(&1.domain == "domain:#{d.id}"))
    end

    test "deletes an entry in redis", context do
      domain = context[:domain]
      {:ok, _} = DomainCache.put(domain)
      {:ok, [1, 1, 0, 1, 0]} = DomainCache.delete(domain.id)
      assert {:ok, nil} == DomainCache.get(domain.id)
    end
  end
end
