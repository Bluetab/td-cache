defmodule TdCache.StructureCacheTest do
  use ExUnit.Case
  alias TdCache.StructureCache
  doctest TdCache.StructureCache

  setup do
    system = %{id: :rand.uniform(100_000_000), external_id: "foo", name: "bar"}
    metadata = %{foo: "bar"}

    structure = %{
      id: :rand.uniform(100_000_000),
      name: "name",
      group: "group",
      type: "type",
      path: ["foo", "bar"],
      metadata: metadata,
      system: system
    }

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
      assert Map.get(s, :system) == Map.drop(structure.system, [:id])
      assert Map.drop(s, [:id, :system]) == Map.drop(structure, [:id, :system])
    end

    test "deletes an entry in redis", context do
      structure = context[:structure]
      assert {:ok, ["OK", 0, 2, "OK", "OK"]} = StructureCache.put(structure)
      assert {:ok, 3} = StructureCache.delete(structure.id)
      assert {:ok, nil} = StructureCache.get(structure.id)
    end
  end
end
