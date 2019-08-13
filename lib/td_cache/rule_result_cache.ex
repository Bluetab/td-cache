defmodule TdCache.RuleResultCache do
  @moduledoc """
  Shared cache for quality rule results.
  """
  alias TdCache.Redix

  @failed_ids "rule_result:ids:failed"
  @props [:date, :implementation_key, :result, :rule_id]

  @doc """
  Creates rule result entries.
  """
  def put(rule_result) do
    reply = put_rule_result(rule_result)
    {:ok, reply}
  end

  @doc """
  Deletes rule result.
  """
  def delete(id) do
    reply = delete_rule_result(id)
    {:ok, reply}
  end

  @doc """
  Deletes rule result.
  """
  def get(id) do
    rule_result = get_rule_result(id)
    {:ok, rule_result}
  end

  @doc """
  Update members in failed_ids set.
  """
  def update_failed_ids(ids) when is_list(ids) do
    reply = update_failed_ids_cache(ids)
    {:ok, reply}
  end

  def update_failed_ids(id) do
    update_failed_ids([id])
  end

  @doc """
  Verifies if the given id is member of failed_ids.
  """
  def member_failed_ids?(id) do
    reply = is_member_failed_ids?(id)
    {:ok, reply}
  end

  @doc """
  Delete set failed_ids.
  """
  def delete_failed_ids do
    reply = delete_failed_ids_from_cache()
    {:ok, reply}
  end

  @doc """
  Delete id from failed_ids set.
  """
  def delete_from_failed_ids(id) do
    reply = delete_from_failed_ids_cache(id)
    {:ok, reply}
  end

  @doc """
  Members from failed ids.
  """
  def members_failed_ids do
    reply = members()
    {:ok, reply}
  end

  defp put_rule_result(%{id: id} = rule_result) do
    Redix.command!([
      "HMSET",
      "rule_result:#{id}",
      Map.take(rule_result, @props)
    ])
  end

  defp get_rule_result(id) do
    {:ok, map} = Redix.read_map("rule_result:#{id}")

    case map do
      nil -> nil
      rule_result -> Map.put(rule_result, :id, id)
    end
  end

  defp update_failed_ids_cache(ids) do
    Redix.command!(["SADD", @failed_ids] ++ ids)
  end

  defp delete_failed_ids_from_cache do
    Redix.command!(["DEL", @failed_ids])
  end

  defp is_member_failed_ids?(id) do
    Redix.command!(["SISMEMBER", @failed_ids, id])
  end

  defp delete_from_failed_ids_cache(id) do
    Redix.command!(["SREM", @failed_ids, id])
  end

  defp delete_rule_result(id) do
    Redix.command!(["DEL", "rule_result:#{id}"])
  end

  defp members do
    Redix.command!(["SMEMBERS", @failed_ids])
  end
end
