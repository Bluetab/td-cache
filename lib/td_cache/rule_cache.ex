defmodule TdCache.RuleCache do
  @moduledoc """
  Shared cache for quality rules.
  """
  alias TdCache.EventStream.Publisher
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands

  ## Client API

  @doc """
  Creates cache entries relating to a given rule.
  """
  def put(rule) do
    put_rule(rule)
  end

  @doc """
  Reads rule information for a given id from cache.
  """
  def get(id) do
    rule = read_rule(id)
    {:ok, rule}
  end

  @doc """
  Counts rules for a given key.
  """
  def count(key) do
    Redis.command(["SCARD", "#{key}:rules"])
  end

  @doc """
  Deletes cache entries relating to a given rule id.
  """
  def delete(id) do
    delete_rule(id)
  end

  ## Private functions

  @props [:active, :name, :updated_at, :business_concept_id]

  defp read_rule(id) do
    {:ok, map} = Redis.read_map("rule:#{id}")

    case map do
      nil -> nil
      rule -> Map.put(rule, :id, id)
    end
  end

  defp put_rule(%{id: id, updated_at: updated_at} = rule) do
    last_updated = Redis.command!(["HGET", "rule:#{id}", :updated_at])

    rule
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_rule(last_updated)
  end

  defp put_rule(%{updated_at: ts}, ts), do: {:ok, []}

  defp put_rule(%{id: id, business_concept_id: business_concept_id} = rule, _last_updated) do
    commands = [
      ["SADD", "business_concept:#{business_concept_id}:rules", "rule:#{id}"],
      Commands.hmset("rule:#{id}", Map.take(rule, @props)),
      ["SADD", "rule:keys", "rule:#{id}"]
    ]

    {:ok, results} = Redis.transaction_pipeline(commands)

    [added, _, _] = results

    unless added == 0 do
      event = %{
        event: "add_rule",
        rule: "rule:#{id}",
        concept: "business_concept:#{business_concept_id}"
      }

      {:ok, _event_id} = Publisher.publish(event, "business_concept:events")
    end

    {:ok, results}
  end

  defp put_rule(%{id: id} = rule, _last_updated) do
    commands = [
      Commands.hmset("rule:#{id}", Map.take(rule, @props)),
      ["SADD", "rule:keys", "rule:#{id}"]
    ]

    Redis.transaction_pipeline(commands)
  end

  def delete_rule(id) do
    {:ok, business_concept_id} = Redis.command(["HGET", "rule:#{id}", "business_concept_id"])
    delete_rule(id, business_concept_id)
  end

  defp delete_rule(id, nil = _business_concept_id) do
    Redis.transaction_pipeline([
      ["DEL", "rule:#{id}"],
      ["SREM", "rule:keys", "rule:#{id}"]
    ])
  end

  defp delete_rule(id, business_concept_id) do
    commands = [
      ["SREM", "business_concept:#{business_concept_id}:rules", "rule:#{id}"],
      ["DEL", "rule:#{id}"],
      ["SREM", "rule:keys", "rule:#{id}"]
    ]

    {:ok, results} = Redis.transaction_pipeline(commands)
    [removed, _, _] = results

    unless removed == 0 do
      event = %{
        event: "remove_rule",
        rule: "rule:#{id}",
        concept: "business_concept:#{business_concept_id}"
      }

      {:ok, _event_id} = Publisher.publish(event, "business_concept:events")
    end

    {:ok, results}
  end
end
