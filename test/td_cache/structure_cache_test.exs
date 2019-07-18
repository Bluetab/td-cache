defmodule TdCache.StructureCacheTest do
  use ExUnit.Case
  alias TdCache.Redix
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

    test "reads an structure external id" do
      system_external_id = "Test System"
      [group, name, field] = ["Test Group", "Test Structure", "Test Field"]
      external_id = Enum.join([group, name, field], ".")
      assert {:ok, 1} = Redix.command(["SADD", "structures:external_ids:#{system_external_id}", external_id])
      assert StructureCache.get_external_id(system_external_id, external_id) == external_id
      unexisting_external_id = Enum.join([group, name, "foo"], ".")
      assert StructureCache.get_external_id(system_external_id, unexisting_external_id) == nil
      assert {:ok, 1} = Redix.command(["SREM", "structures:external_ids:#{system_external_id}", external_id])
    end
  end
end
