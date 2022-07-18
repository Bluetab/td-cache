defmodule TdCache.ImplementationCacheTest do
  use ExUnit.Case

  import Assertions

  alias TdCache.ImplementationCache
  alias TdCache.LinkCache
  alias TdCache.Redix

  setup do
    implementation = %{
      id: 10,
      implementation_key: "key",
      domain_id: 10,
      goal: 8.0,
      minimum: 5.0,
      updated_at: DateTime.utc_now(),
      deleted_at: nil,
      rule_id: nil,
      status: "published"
    }

    on_exit(fn ->
      ImplementationCache.delete(implementation.id)
      Redix.command(["SREM", "implementation:deleted_ids", implementation.id])
    end)

    {:ok, implementation: implementation}
  end

  describe "ImplementationCache" do
    test "writes an implementation entry in redis and reads it back", %{
      implementation: implementation
    } do
      assert {:ok, [9, 0, 1, 0]} = ImplementationCache.put(implementation)

      {:ok, impl} = ImplementationCache.get(implementation.id)

      assert_maps_equal(impl, implementation, [
        :id,
        :implementation_key,
        :domain_id,
        :deleted_at,
        :goal,
        :minimum,
        :rule_id,
        :status
      ])

      refute Map.has_key?(impl, :execution_result_info)
    end

    test "writes an implementation entry in redis and reads it back with execution_result_info",
         %{
           implementation: implementation
         } do
      implementation =
        Map.put(implementation, :execution_result_info, %{
          errors: 10
        })

      {:ok, _} = ImplementationCache.put(implementation)

      {:ok, impl} = ImplementationCache.get(implementation.id)

      assert not is_nil(impl)
      assert impl.id == implementation.id
      assert impl.execution_result_info.errors == 10
    end

    test "writes an implementation entry in redis and reads it back with rule", %{
      implementation: implementation
    } do
      implementation =
        implementation
        |> Map.put(:rule, %{
          id: 10,
          name: "rule_name",
          updated_at: DateTime.utc_now()
        })
        |> Map.put(:rule_id, 10)

      {:ok, _} = ImplementationCache.put(implementation)

      {:ok, impl} = ImplementationCache.get(implementation.id)

      assert not is_nil(impl)
      assert impl.id == implementation.id
      assert impl.rule_id == 10
      assert impl.rule.name == "rule_name"
    end

    test "updates an implementation already cached in redis when updated_at has changed", %{
      implementation: implementation
    } do
      {:ok, _} = ImplementationCache.put(implementation)
      {:ok, impl} = ImplementationCache.get(implementation.id)
      assert not is_nil(impl)
      domain_id = 20

      updated_implementation =
        implementation
        |> Map.put(:domain_id, domain_id)
        |> Map.put(:updated_at, DateTime.utc_now())

      {:ok, _} = ImplementationCache.put(updated_implementation)
      {:ok, impl} = ImplementationCache.get(implementation.id)
      assert impl.domain_id == domain_id
    end

    test "does not update an implementation already cached in redis having same update_at value",
         %{
           implementation: implementation
         } do
      {:ok, _} = ImplementationCache.put(implementation)
      {:ok, impl} = ImplementationCache.get(implementation.id)
      assert not is_nil(impl)
      goal = 2.0
      updated_implementation = Map.put(implementation, :goal, goal)
      {:ok, _} = ImplementationCache.put(updated_implementation)
      {:ok, impl} = ImplementationCache.get(implementation.id)
      assert impl.goal == 8
    end

    test "updates an implementation already cached in redis when deleted_at has changed", %{
      implementation: implementation
    } do
      assert {:ok, _} = ImplementationCache.put(implementation)
      assert {:ok, imp} = ImplementationCache.get(implementation.id)

      assert imp
             |> Map.get(:deleted_at)
             |> is_nil()

      deleted_at = DateTime.utc_now()
      implementation = Map.put(implementation, :deleted_at, deleted_at)
      assert {:ok, _} = ImplementationCache.put(implementation)
      assert {:ok, imp} = ImplementationCache.get(implementation.id)

      assert Map.get(imp, :deleted_at) == "#{deleted_at}"
    end

    test "deletes an entry in redis", %{implementation: implementation} do
      assert {:ok, _} = ImplementationCache.put(implementation)
      assert {:ok, [1, 1, 1]} = ImplementationCache.delete(implementation.id)
      assert {:ok, nil} = ImplementationCache.get(implementation.id)
    end

    test "deletes an entry in redis with execution_result_info", %{implementation: implementation} do
      implementation =
        Map.put(implementation, :execution_result_info, %{
          errors: 1,
          records: 2
        })

      assert {:ok, _} = ImplementationCache.put(implementation)
      assert {:ok, [2, 1, 1]} = ImplementationCache.delete(implementation.id)
      assert {:ok, nil} = ImplementationCache.get(implementation.id)
    end

    test "lists structure ids referenced in links", %{implementation: %{id: id}} do
      create_link(id)
      assert ImplementationCache.referenced_ids() == [id]
    end
  end

  defp create_link(implementation_id) do
    id = System.unique_integer([:positive])

    concept_id = 8

    on_exit(fn -> LinkCache.delete(id, publish: false) end)

    LinkCache.put(
      %{
        id: id,
        source_id: implementation_id,
        target_id: concept_id,
        updated_at: DateTime.utc_now(),
        source_type: "implementation",
        target_type: "business_concept"
      },
      publish: false
    )
  end
end
