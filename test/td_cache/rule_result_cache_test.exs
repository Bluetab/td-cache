defmodule TdCache.RuleResultCacheTest do
  use ExUnit.Case
  import TdCache.TestOperators
  alias TdCache.RuleResultCache
  doctest TdCache.RuleResultCache

  setup do
    rule_result = %{
      id: :rand.uniform(100_000_000),
      implementation_key: "key",
      date: DateTime.utc_now(),
      rule_id: :rand.uniform(100_000_000),
      result: 80
    }

    {:ok, "OK"} = RuleResultCache.put(rule_result)
    {:ok, 1} = RuleResultCache.update_failed_ids([rule_result.id])

    on_exit(fn ->
      RuleResultCache.delete(rule_result.id)
      RuleResultCache.delete_from_failed_ids(rule_result.id)
    end)

    {:ok, rule_result: rule_result}
  end

  describe "RuleResultCache" do
    test "writes a rule result entry in redis and reads it back", context do
      rule_result = context[:rule_result]
      RuleResultCache.put(rule_result)
      {:ok, s} = RuleResultCache.get(rule_result.id)
      assert s <~> rule_result
    end

    test "deletes a rule result and does not exist", context do
      rule_result = context[:rule_result]
      assert {:ok, 1} = RuleResultCache.delete(rule_result.id)
      assert {:ok, nil} = RuleResultCache.get(rule_result.id)
    end

    test "adds a rule result to the failed set and exists", context do
      rule_result = context[:rule_result]
      assert {:ok, _} = RuleResultCache.update_failed_ids(rule_result.id)
      assert {:ok, 1} = RuleResultCache.member_failed_ids?(rule_result.id)
    end

    test "removes and entry from failed set", context do
      rule_result = context[:rule_result]
      assert {:ok, 1} = RuleResultCache.delete_from_failed_ids(rule_result.id)
      assert {:ok, 0} = RuleResultCache.member_failed_ids?(rule_result.id)
    end
  end
end
