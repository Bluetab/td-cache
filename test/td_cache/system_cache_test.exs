defmodule TdCache.SystemCacheTest do
  use ExUnit.Case
  alias TdCache.Redix
  alias TdCache.SystemCache
  doctest TdCache.SystemCache

  setup do
    system = %{id: System.unique_integer([:positive]), external_id: "foo", name: "bar"}

    on_exit(fn ->
      SystemCache.delete(system.id)
      Redix.command(["DEL", "systems:ids_external_ids"])
    end)

    {:ok, system: system}
  end

  describe "SystemCache" do
    test "writes a system entry in redis and reads it back", context do
      system = context[:system]
      {:ok, _} = SystemCache.put(system)
      {:ok, s} = SystemCache.get(system.id)
      assert s == system
    end

    test "external_id_to_id_map gets id", context do
      system = context[:system]
      prev_external_id = system.external_id
      {:ok, _} = SystemCache.put(system)
      {:ok, s} = SystemCache.get(system.id)
      {:ok, m} = SystemCache.external_id_to_id_map()

      assert s.id == Map.get(m, system.external_id)
      assert s == system

      external_id = "bar"
      system = Map.put(system, :external_id, external_id)

      {:ok, _} = SystemCache.put(system)
      {:ok, s} = SystemCache.get(system.id)
      {:ok, m} = SystemCache.external_id_to_id_map()

      assert s.id == Map.get(m, system.external_id)
      assert is_nil(Map.get(m, prev_external_id))
      assert s == system

      {:ok, _} = SystemCache.delete(system.id)
      {:ok, m} = SystemCache.external_id_to_id_map()
      assert is_nil(Map.get(m, external_id))
    end

    test "deletes an entry in redis", context do
      system = context[:system]
      {:ok, _} = SystemCache.put(system)
      {:ok, _} = SystemCache.delete(system.id)
      assert {:ok, nil} == SystemCache.get(system.id)
    end
  end
end
