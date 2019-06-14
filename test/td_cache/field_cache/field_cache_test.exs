defmodule TdCache.FieldCache.Test do
  use ExUnit.Case
  alias TdCache.FieldCache
  doctest TdCache.FieldCache

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

    field = %{
      id: :rand.uniform(100_000_000),
      structure: structure
    }

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

      assert Map.take(f, [:group, :metadata, :name, :path, :type]) ==
               Map.take(field.structure, [:group, :metadata, :name, :path, :type])

      assert f.system == Map.take(field.structure.system, [:name, :external_id])
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
