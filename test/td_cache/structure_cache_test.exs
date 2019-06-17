defmodule TdCache.StructureCacheTest do
  use ExUnit.Case
  alias TdCache.StructureCache
  alias TdCache.SystemCache
  doctest TdCache.StructureCache

  setup do
    system = %{id: :rand.uniform(100_000_000), external_id: "foo", name: "bar"}

    structure = %{
      id: :rand.uniform(100_000_000),
      name: "name",
      group: "group",
      type: "type",
      path: ["foo", "bar"],
      system: system
    }

    on_exit(fn ->
      StructureCache.delete(structure.id)
      SystemCache.delete(system.id)
    end)

    {:ok, structure: structure}
  end

  describe "StructureCache" do
    test "starts automatically" do
      assert Process.whereis(StructureCache)
    end

    test "writes a structure entry in redis and reads it back", context do
      structure = context[:structure]
      {:ok, _} = StructureCache.put(structure)
      {:ok, s} = StructureCache.get(structure.id)
      assert s == structure
    end

    test "deletes an entry in redis", context do
      structure = context[:structure]
      assert {:ok, ["OK", 1, 0, 2, "OK"]} = StructureCache.put(structure)
      assert {:ok, [2, 1]} = StructureCache.delete(structure.id)
      assert {:ok, nil} = StructureCache.get(structure.id)
    end
  end
end
