defmodule TdCache.FieldCacheTest do
  use ExUnit.Case
  alias TdCache.FieldCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream
  alias TdCache.StructureCache
  alias TdCache.SystemCache
  doctest TdCache.FieldCache

  setup do
    now = DateTime.utc_now()

    system = %{id: :rand.uniform(100_000_000), external_id: "foo", name: "bar"}

    structure = %{
      id: :rand.uniform(100_000_000),
      name: "name",
      group: "group",
      type: "type",
      path: ["foo", "bar"],
      system: system,
      updated_at: now
    }

    field = %{
      id: :rand.uniform(100_000_000),
      structure: structure,
      updated_at: now
    }

    {:ok, _} = SystemCache.put(system)

    on_exit(fn ->
      FieldCache.delete(field.id)
      StructureCache.delete(structure.id)
      SystemCache.delete(system.id)
      Redix.command(["DEL", "data_field:events"])
    end)

    {:ok, field: field}
  end

  describe "FieldCache" do
    test "writes a field entry in redis and reads it back", context do
      field = context[:field]
      assert {:ok, ["OK", 1]} = FieldCache.put(field)

      {:ok, f} = FieldCache.get(field.id)

      assert Map.take(f, [:group, :name, :path, :type]) ==
               Map.take(field.structure, [:group, :name, :path, :type])

      assert f.system == Map.take(field.structure.system, [:name, :external_id, :id])
      assert f.structure_id == to_string(field.structure.id)
    end

    test "emits an event when a new field is cached", context do
      field = context[:field]
      {:ok, _} = FieldCache.put(field)

      assert {:ok, [event]} = Stream.read(["data_field:events"], transform: true)
      assert event.event == "migrate_field"
      assert event.field_id == "#{field.id}"
      assert event.structure_id == "#{field.structure.id}"
    end

    test "deletes an entry in redis", context do
      field = context[:field]
      assert {:ok, ["OK", 1]} = FieldCache.put(field)
      assert {:ok, [1, 1]} = FieldCache.delete(field.id)
      assert {:ok, nil} = FieldCache.get(field.id)
    end

    test "emits an event if a field has no structure", context do
      field = context[:field] |> Map.delete(:structure)
      assert {:error, :missing_structure} = FieldCache.put(field)

      assert {:ok, [event]} = Stream.read(["data_field:events"], transform: true)
      assert event.event == "unlink_field"
      assert event.field_id == "#{field.id}"
    end

    test "reads an external id" do
      system_external_id = "Test System"
      [group, name, field] = ["Test Group", "Test Structure", "Test Field"]
      external_id = Enum.join([group, name, field], ".")
      assert {:ok, 1} = Redix.command(["SADD", "structures:external_ids:#{system_external_id}", external_id])
      assert FieldCache.get_external_id(system_external_id, external_id) == external_id
      unexisting_external_id = Enum.join([group, name, "foo"], ".")
      assert FieldCache.get_external_id(system_external_id, unexisting_external_id) == nil
      assert {:ok, 1} = Redix.command(["SREM", "structures:external_ids:#{system_external_id}", external_id])
    end
  end
end
