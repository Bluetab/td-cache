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
      updated_at: DateTime.utc_now()
    }

    {:ok, _} = SystemCache.put(system)

    on_exit(fn ->
      StructureCache.delete(structure.id)
      SystemCache.delete(system.id)
    end)

    {:ok, structure: structure, system: system}
  end

  describe "StructureCache" do
    test "writes a structure entry in redis and reads it back", context do
      structure = context[:structure]
      {:ok, _} = StructureCache.put(structure)
      {:ok, s} = StructureCache.get(structure.id)
      assert not is_nil(s)
      assert s.id == structure.id
      assert s.name == structure.name
      assert s.group == structure.group
      assert s.path == structure.path
      assert s.type == structure.type
    end

    test "writes a structure entry with system in redis and reads it back", context do
      system = context[:system]

      structure =
        context[:structure]
        |> Map.put(:system_id, system.id)

      {:ok, _} = StructureCache.put(structure)
      {:ok, s} = StructureCache.get(structure.id)
      assert not is_nil(s)
      assert s.id == structure.id
      assert s.name == structure.name
      assert s.group == structure.group
      assert s.path == structure.path
      assert s.type == structure.type
      assert s.system_id == "#{system.id}"
      assert s.system == system
    end

    test "deletes an entry in redis", context do
      structure = context[:structure]
      assert {:ok, ["OK", 1, 0, 2]} = StructureCache.put(structure)
      assert {:ok, [2, 1]} = StructureCache.delete(structure.id)
      assert {:ok, nil} = StructureCache.get(structure.id)
    end
  end
end
