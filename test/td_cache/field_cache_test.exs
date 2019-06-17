defmodule TdCache.FieldCacheTest do
  use ExUnit.Case
  alias TdCache.FieldCache
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

    on_exit(fn ->
      FieldCache.delete(field.id)
      StructureCache.delete(structure.id)
      SystemCache.delete(system.id)
    end)

    {:ok, field: field}
  end

  describe "FieldCache" do
    test "starts automatically" do
      assert Process.whereis(FieldCache)
    end

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
      assert {:ok, "OK"} = FieldCache.put(field)
      assert {:ok, 1} = FieldCache.delete(field.id)
      assert {:ok, nil} = FieldCache.get(field.id)
    end
  end
end
