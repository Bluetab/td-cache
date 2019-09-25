defmodule TdCache.RuleCacheTest do
  use ExUnit.Case
  import TdCache.TestOperators
  alias TdCache.ConceptCache
  alias TdCache.RuleCache
  doctest TdCache.RuleCache

  setup do
    concept = %{id: :rand.uniform(100_000_000), name: "foo"}

    rule = %{
      id: :rand.uniform(100_000_000),
      active: true,
      updated_at: DateTime.utc_now(),
      business_concept_id: concept.id,
      minimum: 20
    }

    {:ok, _} = ConceptCache.put(concept)

    on_exit(fn ->
      RuleCache.delete(rule.id)
      ConceptCache.delete(concept.id)
    end)

    {:ok, rule: rule, concept: concept}
  end

  describe "RuleCache" do
    test "writes a rule entry in redis and reads it back", context do
      rule = context[:rule]
      {:ok, [1, "OK", 1]} = RuleCache.put(rule)
      {:ok, s} = RuleCache.get(rule.id)
      assert s <~> rule
    end

    test "adds a rule to the concept's rules", context do
      rule = context[:rule]
      concept = context[:concept]
      {:ok, [1, "OK", 1]} = RuleCache.put(rule)
      {:ok, c} = ConceptCache.get(concept.id)
      assert c.rule_count == 1
    end

    test "deletes an entry in redis", context do
      rule = context[:rule]
      {:ok, _} = RuleCache.put(rule)
      {:ok, _} = RuleCache.delete(rule.id)
      assert {:ok, nil} == RuleCache.get(rule.id)
    end

    test "returns the rule count of the concept", context do
      rule = context[:rule]
      {:ok, _} = RuleCache.put(rule)
      assert {:ok, 1} == RuleCache.count("business_concept:#{rule.business_concept_id}")

      {:ok, _} = RuleCache.delete(rule.id)
      assert {:ok, 0} == RuleCache.count("business_concept:#{rule.business_concept_id}")
    end

    test "returns keys in cache", context do
      rule = context[:rule]
      {:ok, _} = RuleCache.put(rule)
      assert {:ok, keys} = RuleCache.read_rule_keys()
      assert Enum.any?(keys, fn k -> k == "rule:#{Map.get(rule, :id)}" end) 
    end
  end
end
