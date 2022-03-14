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

    test "get_by_external_id returns ok/error tuple", %{system: system} do
      assert {:ok, _} = SystemCache.put(system)
      assert SystemCache.get_by_external_id("missing") == {:error, :not_found}
      assert {:ok, s} = SystemCache.get_by_external_id(system.external_id)
      assert s == system
    end

    test "deletes an entry in redis", context do
      system = context[:system]
      {:ok, _} = SystemCache.put(system)
      {:ok, _} = SystemCache.delete(system.id)
      assert {:ok, nil} == SystemCache.get(system.id)
    end
  end
end
