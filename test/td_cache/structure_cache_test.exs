defmodule TdCache.StructureCacheTest do
  use ExUnit.Case

  alias TdCache.EventStream.Publisher
  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream
  alias TdCache.StructureCache
  alias TdCache.SystemCache

  doctest TdCache.StructureCache

  setup do
    system = %{id: :rand.uniform(100_000_000), external_id: "foo", name: "bar"}

    structure = %{
      id: :rand.uniform(100_000_000),
      name: "name",
      external_id: "ext_id",
      group: "group",
      type: "type",
      path: ["foo", "bar"],
      updated_at: DateTime.utc_now(),
      metadata: %{"alias" => "source_alias"},
      system_id: system.id,
      deleted_at: DateTime.utc_now()
    }

    {:ok, _} = SystemCache.put(system)

    on_exit(fn ->
      StructureCache.delete(structure.id)
      SystemCache.delete(system.id)
      Redix.command(["SREM", "data_structure:deleted", "data_structure:#{structure.id}"])
    end)

    {:ok, structure: structure, system: system}
  end

  describe "StructureCache" do
    test "writes a structure entry in redis and reads it back", %{structure: structure} do
      {:ok, _} = StructureCache.put(structure)
      {:ok, s} = StructureCache.get(structure.id)
      assert not is_nil(s)
      assert s.id == structure.id
      assert s.name == structure.name
      assert s.external_id == structure.external_id
      assert s.group == structure.group
      assert s.path == structure.path
      assert s.type == structure.type
      assert s.metadata == structure.metadata
      assert not is_nil(s.deleted_at)
    end

    test "writes a structure entry with system in redis and reads it back", %{
      structure: structure,
      system: system
    } do
      {:ok, _} = StructureCache.put(structure)
      {:ok, s} = StructureCache.get(structure.id)
      assert not is_nil(s)
      assert s.id == structure.id
      assert s.name == structure.name
      assert s.external_id == structure.external_id
      assert s.group == structure.group
      assert s.path == structure.path
      assert s.type == structure.type
      assert s.metadata == structure.metadata
      assert s.system_id == "#{system.id}"
      assert s.system == system
    end

    test "returns an empty map for metadata if not present", %{structure: structure} do
      structure = Map.delete(structure, :metadata)
      assert {:ok, _} = StructureCache.put(structure)
      assert {:ok, s} = StructureCache.get(structure.id)
      assert s.metadata == %{}
    end

    test "updates a structure already cached in redis when updated_at has changed", %{
      structure: structure
    } do
      {:ok, _} = StructureCache.put(structure)
      {:ok, s} = StructureCache.get(structure.id)
      assert not is_nil(s)

      updated_structure =
        structure
        |> Map.put(:external_id, "new_ext_id")
        |> Map.put(:updated_at, DateTime.utc_now())

      {:ok, _} = StructureCache.put(updated_structure)
      {:ok, s} = StructureCache.get(structure.id)
      assert s.external_id == "new_ext_id"
    end

    test "does not update a structure already cached in redis having same update_at value", %{
      structure: structure
    } do
      {:ok, _} = StructureCache.put(structure)
      {:ok, s} = StructureCache.get(structure.id)
      assert not is_nil(s)
      updated_structure = Map.put(structure, :external_id, "new_ext_id")
      {:ok, _} = StructureCache.put(updated_structure)
      {:ok, s} = StructureCache.get(structure.id)
      assert s.external_id == "ext_id"
    end

    test "updates a structure already cached in redis when deleted_at has changed", %{
      structure: structure
    } do
      assert {:ok, _} = StructureCache.put(structure)
      assert {:ok, s} = StructureCache.get(structure.id)
      assert Map.get(s, :deleted_at) == "#{structure.deleted_at}"

      structure = Map.put(structure, :deleted_at, nil)
      assert {:ok, _} = StructureCache.put(structure)
      assert {:ok, s} = StructureCache.get(structure.id)
      assert Map.get(s, :deleted_at) == ""
    end

    test "deletes an entry in redis", %{structure: structure} do
      assert {:ok, ["OK", 1, 0, 2, 1]} = StructureCache.put(structure)
      assert {:ok, [2, 1, 0]} = StructureCache.delete(structure.id)
      assert {:ok, nil} = StructureCache.get(structure.id)
    end

    test "lists structure ids referenced in rule events", %{structure: %{id: id}} do
      publish_event(%{
        stream: "data_structure:events",
        event: "add_rule_implementation_link",
        structure_id: id
      })

      assert StructureCache.referenced_ids() == [id]
    end

    test "lists structure ids referenced in linkss", %{structure: %{id: id}} do
      create_link(id)
      assert StructureCache.referenced_ids() == [id]
    end
  end

  defp publish_event(event) do
    {:ok, event_id} = Publisher.publish(event)

    on_exit(fn ->
      Stream.delete_events("data_structure:events", [event_id])
      Stream.delete_if_empty("data_structure:events")
    end)
  end

  defp create_link(structure_id) do
    id = :rand.uniform(100_000_000)

    on_exit(fn -> LinkCache.delete(id, publish: false) end)

    LinkCache.put(
      %{
        id: id,
        source_id: structure_id,
        target_id: structure_id,
        updated_at: DateTime.utc_now(),
        source_type: "data_structure",
        target_type: "data_structure"
      },
      publish: false
    )
  end
end
