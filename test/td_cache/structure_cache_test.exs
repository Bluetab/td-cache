defmodule TdCache.StructureCacheTest do
  use ExUnit.Case

  import Assertions

  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.StructureCache
  alias TdCache.SystemCache

  doctest TdCache.StructureCache

  setup do
    system = %{id: System.unique_integer([:positive]), external_id: "foo", name: "bar"}

    structure = %{
      id: System.unique_integer([:positive]),
      name: "name",
      external_id: "ext_id",
      group: "group",
      type: "type",
      path: ["foo", "bar"],
      updated_at: DateTime.utc_now(),
      metadata: %{"alias" => "source_alias"},
      system_id: system.id,
      domain_ids: [1, 2],
      deleted_at: DateTime.utc_now()
    }

    {:ok, _} = SystemCache.put(system)

    on_exit(fn ->
      StructureCache.delete(structure.id)
      SystemCache.delete(system.id)
      Redix.command(["SREM", "data_structure:deleted_ids", structure.id])
    end)

    [structure: structure, system: system]
  end

  describe "StructureCache" do
    test "writes a structure entry in redis and reads it back", %{structure: structure} do
      {:ok, _} = StructureCache.put(structure)
      {:ok, s} = StructureCache.get(structure.id)

      assert_structs_equal(s, structure, [
        :domain_ids,
        :external_id,
        :group,
        :id,
        :metadata,
        :name,
        :path,
        :type
      ])

      assert s.deleted_at == "#{structure.deleted_at}"
    end

    test "writes a structure entry with system in redis and reads it back", %{
      structure: structure,
      system: system
    } do
      {:ok, _} = StructureCache.put(structure)
      {:ok, s} = StructureCache.get(structure.id)

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
      assert s

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
      assert s
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

    test "updates a structure already cached in redis when domain_ids has changed", %{
      structure: structure
    } do
      assert {:ok, _} = StructureCache.put(structure)
      assert {:ok, []} = StructureCache.put(structure)

      Redix.command!(["HDEL", "data_structure:#{structure.id}", "domain_ids"])
      assert {:ok, [1, 9, 0, 0, 1, 2]} = StructureCache.put(structure)

      assert {:ok, s} = StructureCache.get(structure.id)
      assert_structs_equal(structure, s, [:domain_ids])
    end

    test "deletes an entry in redis", %{structure: structure} do
      assert {:ok, [0, 9, 1, 1, 0, 2]} = StructureCache.put(structure)
      assert {:ok, [2, 1, 0]} = StructureCache.delete(structure.id)
      assert {:ok, nil} = StructureCache.get(structure.id)
    end

    test "lists structure ids referenced in linkss", %{structure: %{id: id}} do
      create_link(id)
      assert StructureCache.referenced_ids() == [id]
    end
  end

  defp create_link(structure_id) do
    id = System.unique_integer([:positive])

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
