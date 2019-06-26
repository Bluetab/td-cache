defmodule TdCache.FieldCacheTest do
  use ExUnit.Case
  alias TdCache.FieldCache
  alias TdCache.Redix
  alias TdCache.StructureCache
  alias TdCache.SystemCache
  doctest TdCache.FieldCache

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

    field = %{
      id: :rand.uniform(100_000_000),
      structure: structure
    }

    {:ok, _} = SystemCache.put(system)

    on_exit(fn ->
      FieldCache.delete(field.id)
      StructureCache.delete(structure.id)
      SystemCache.delete(system.id)
    end)

    {:ok, field: field}
  end

  describe "FieldCache" do
    test "writes a field entry in redis and reads it back", context do
      field = context[:field]
      {:ok, _} = FieldCache.put(field)
      {:ok, f} = FieldCache.get(field.id)

      assert Map.take(f, [:group, :name, :path, :type]) ==
               Map.take(field.structure, [:group, :name, :path, :type])

      assert f.system == Map.take(field.structure.system, [:name, :external_id, :id])
      assert f.structure_id == to_string(field.structure.id)
    end

    test "deletes an entry in redis", context do
      field = context[:field]
      assert {:ok, [1, 1]} = FieldCache.put(field)
      assert {:ok, [1, 1]} = FieldCache.delete(field.id)
      assert {:ok, nil} = FieldCache.get(field.id)
    end

    test "reads an external id" do
      [system, group, name, field] = ["Test System", "Test Group", "Test Structure", "Test Field"]
      external_id = Enum.join([system, group, name, field], ".")
      assert {:ok, 1} = Redix.command(["SADD", "data_fields:external_ids", external_id])
      assert FieldCache.get_external_id(system, group, name, field) == external_id
      assert FieldCache.get_external_id(system, group, name, "foo") == nil
      Redix.command(["SREM", "data_fields:external_ids", external_id])
    end
  end
end
