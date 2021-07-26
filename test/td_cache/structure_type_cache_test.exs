defmodule TdCache.StructureTypeCacheTest do
  use ExUnit.Case
  alias TdCache.StructureTypeCache
  doctest TdCache.StructureTypeCache

  setup do
    structure_type = %{
      id: System.unique_integer([:positive]),
      structure_type: "doc",
      template_id: 1,
      translation: "docu"
    }

    on_exit(fn ->
      StructureTypeCache.delete(structure_type.id)
    end)

    {:ok, structure_type: structure_type}
  end

  describe "StructureTypeCache" do
    test "writes a structure type entry in redis and reads it back", context do
      structure_type = context[:structure_type]
      {:ok, _} = StructureTypeCache.put(structure_type)
      {:ok, s_t} = StructureTypeCache.get(structure_type.id)
      assert s_t.id == structure_type.id
      assert s_t.structure_type == structure_type.structure_type
      assert s_t.template_id == "#{structure_type.template_id}"
      assert s_t.translation == structure_type.translation
    end

    test "get_by_type return a map with structure_type info", context do
      structure_type = context[:structure_type]
      {:ok, _} = StructureTypeCache.put(structure_type)
      {:ok, s_t} = StructureTypeCache.get_by_type(structure_type.structure_type)
      assert s_t.id == structure_type.id
      assert s_t.structure_type == structure_type.structure_type
      assert s_t.template_id == "#{structure_type.template_id}"
      assert s_t.translation == structure_type.translation
    end

    test "deletes an entry in redis", context do
      structure_type = context[:structure_type]
      {:ok, _} = StructureTypeCache.put(structure_type)
      {:ok, _} = StructureTypeCache.delete(structure_type.id)
      assert {:ok, nil} == StructureTypeCache.get(structure_type.id)
    end
  end
end
