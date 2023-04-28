defmodule TdCache.ImplementationCacheTest do
  use ExUnit.Case

  import TdCache.TestOperators
  import TdCache.Factory
  import Assertions

  alias TdCache.ImplementationCache
  alias TdCache.LinkCache
  alias TdCache.Redix

  setup do
    implementation = build(:implementation)

    on_exit(fn ->
      ImplementationCache.delete(implementation.id)
      Redix.command(["SREM", "implementation:deleted_ids", implementation.id])
      Redix.command(["DEL", "relation_impl_id_to_impl_ref"])
    end)

    {:ok, implementation: implementation}
  end

  describe "ImplementationCache" do
    test "writes an implementation entry in redis and reads it back", %{
      implementation: implementation
    } do
      assert {:ok, [10, 0, 1, 0]} = ImplementationCache.put(implementation)

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

    test "writes implementations entries in redis and list implementations keys", %{
      implementation: %{id: impl_id} = implementation
    } do
      %{id: impl2_id} = impl2 = build(:implementation)
      %{id: impl3_id} = impl3 = build(:implementation)
      assert {:ok, [10, 0, 1, 0]} = ImplementationCache.put(implementation)
      assert {:ok, [10, 0, 1, 0]} = ImplementationCache.put(impl2)
      assert {:ok, [10, 0, 1, 0]} = ImplementationCache.put(impl3)

      assert [
               "implementation:#{impl_id}",
               "implementation:#{impl2_id}",
               "implementation:#{impl3_id}"
             ]
             <|> ImplementationCache.list()
    end

    test "write relation between implementation_id and implementation_ref" do
      assert 3 =
               ImplementationCache.put_relation_impl_id_and_impl_ref([
                 111,
                 111,
                 333,
                 333,
                 222,
                 222
               ])

      assert 0 = ImplementationCache.put_relation_impl_id_and_impl_ref([])
    end

    test "get relation between implementation_id and implementation_ref" do
      relation_ids = [111, 111, 333, 333, 222, 222]
      assert 3 = ImplementationCache.put_relation_impl_id_and_impl_ref(relation_ids)
      relation_cache_ids = ImplementationCache.get_relation_impl_id_and_impl_ref()
      string_relation_ids = relation_cache_ids |> Enum.map(&String.to_integer(&1))
      assert relation_ids == string_relation_ids
    end

    test "delete relation between implementation_id and implementation_ref" do
      assert 3 =
               ImplementationCache.put_relation_impl_id_and_impl_ref([
                 111,
                 111,
                 333,
                 333,
                 222,
                 222
               ])

      assert 1 = ImplementationCache.delete_relation_impl_id_and_impl_ref()
      assert [] = ImplementationCache.get_relation_impl_id_and_impl_ref()
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

    test "lists implementation ids referenced in links", %{implementation: %{id: id}} do
      create_link(id)
      assert ImplementationCache.referenced_ids("implementation_ref") == [id]
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
        source_type: "implementation_ref",
        target_type: "business_concept"
      },
      publish: false
    )
  end
end
