defmodule TdCache.ImplementationCacheTest do
  use ExUnit.Case

  import TdCache.TestOperators

  alias TdCache.ImplementationCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream
  alias TdCache.StructureCache
  alias TdCache.SystemCache

  doctest TdCache.ImplementationCache

  @stream "data_structure:events"

  setup_all do
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
      Redix.command(["SREM", "data_structure:deleted_ids", structure.id])
    end)

    [structure: structure, system: system]
  end

  setup tags do
    %{structure: %{id: structure_id}} = tags

    implementation = %{
      id: :rand.uniform(100_000_000),
      updated_at: DateTime.utc_now(),
      structure_ids: [structure_id]
    }

    on_exit(fn ->
      Redix.command(["DEL", @stream])
      ImplementationCache.delete(implementation.id)
    end)

    [implementation: implementation]
  end

  describe "ImplementationCache" do
    test "writes an implementation entry in redis and reads it back", %{
      implementation: implementation
    } do
      assert {:ok, [1, "OK", 0, 1, [_], 1, 1]} = ImplementationCache.put(implementation)
      assert {:ok, s} = ImplementationCache.get(implementation.id)
      assert s <~> implementation
    end

    test "writes an implementation entry with no structure_ids redis and reads it back", %{
      implementation: implementation
    } do
      implementation = Map.put(implementation, :structure_ids, [])
      assert {:ok, [1, "OK", 0]} = ImplementationCache.put(implementation)
      assert {:ok, s} = ImplementationCache.get(implementation.id)
      assert s <~> implementation
    end

    test "refreshes structure ids", %{implementation: implementation} do
      implementation = %{implementation | structure_ids: [456, 999]}

      assert {:ok, _} = ImplementationCache.put(implementation)

      implementation = %{
        implementation
        | structure_ids: [123, 456, 789],
          updated_at: DateTime.utc_now()
      }

      assert {:ok, [0, "OK", 0, 3, new_ids, 3, 1]} = ImplementationCache.put(implementation)
      assert new_ids == ["123", "789"]
      assert {:ok, s} = ImplementationCache.get(implementation.id)
      assert s <~> implementation
    end

    test "publishes an event when a structure_id is added", %{implementation: implementation} do
      assert {:ok, _} =
               implementation
               |> Map.put(:structure_ids, [123, 456, 789])
               |> ImplementationCache.put()

      assert {:ok, [e]} = Stream.read(:redix, [@stream], transform: true)
      assert %{event: "add_rule_implementation_link", structure_ids: "123,456,789"} = e

      assert {:ok, _} =
               implementation
               |> Map.put(:structure_ids, [123, 222, 789, 333])
               |> Map.put(:updated_at, DateTime.utc_now())
               |> ImplementationCache.put()

      assert {:ok, [_, e]} = Stream.read(:redix, [@stream], transform: true)
      assert %{event: "add_rule_implementation_link", structure_ids: "222,333"} = e
    end

    test "deletes an entry in redis", %{implementation: implementation} do
      assert {:ok, _} = ImplementationCache.put(implementation)
      assert {:ok, [1, 1, 1]} = ImplementationCache.delete(implementation.id)
      assert {:ok, nil} == ImplementationCache.get(implementation.id)
    end

    test "lists all keys", %{implementation: implementation} do
      assert {:ok, []} = ImplementationCache.keys()
      assert {:ok, _} = ImplementationCache.put(implementation)
      assert {:ok, [_]} = ImplementationCache.keys()
    end

    test "lists all entries", %{implementation: implementation} do
      assert [] = ImplementationCache.list()
      assert {:ok, _} = ImplementationCache.put(implementation)
      assert [s] = ImplementationCache.list()
      assert s <~> implementation
    end

    test "lists referenced structure ids", %{implementation: implementation, structure: %{id: id}} do
      assert [] = ImplementationCache.referenced_structure_ids()
      assert {:ok, _} = ImplementationCache.put(implementation)
      assert [^id] = ImplementationCache.referenced_structure_ids()
    end
  end
end
