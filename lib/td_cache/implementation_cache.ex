defmodule TdCache.ImplementationCache do
  @moduledoc """
  Shared cache for quality rule implementations.
  """

  alias TdCache.EventStream.Publisher
  alias TdCache.Redix

  ## Client API

  @doc """
  Creates cache entries relating to a given rule implementation.
  """
  def put(implementation) do
    put_implementation(implementation)
  end

  @doc """
  Reads rule implementation information for a given id from cache.
  """
  def get(id) do
    implementation = read_implementation(id)
    {:ok, implementation}
  end

  @doc """
  Deletes cache entries relating to a given rule implementation id.
  """
  def delete(id) do
    delete_implementation(id)
  end

  def list do
    case keys() do
      {:ok, ks} ->
        ks
        |> Enum.map(fn "implementation:" <> id -> read_implementation(id) end)
        |> Enum.reject(&is_nil/1)
    end
  end

  def referenced_structure_ids do
    case keys() do
      {:ok, []} ->
        []

      {:ok, ks} ->
        ["SINTER" | Enum.map(ks, fn k -> k <> ":structure_ids" end)]
        |> Redix.command()
        |> case do
          {:ok, ids} -> Enum.map(ids, &String.to_integer/1)
        end
    end
  end

  @doc """
  List all rule implementation keys.
  """
  def keys do
    Redix.command(["SMEMBERS", "implementation:keys"])
  end

  ## Private functions

  @props [:implementation_key, :rule_id, :updated_at]

  defp read_implementation(id) do
    with {:ok, %{} = map} <- Redix.read_map("implementation:#{id}"),
         {:ok, structure_ids} <- Redix.command(["SMEMBERS", "implementation:#{id}:structure_ids"]) do
      map
      |> Map.put(:id, id)
      |> Map.put(:structure_ids, Enum.map(structure_ids, &String.to_integer/1))
    else
      {:ok, nil} -> nil
    end
  end

  defp put_implementation(%{id: id, updated_at: updated_at} = implementation) do
    last_updated = Redix.command!(["HGET", "implementation:#{id}", :updated_at])

    implementation
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_implementation(last_updated)
  end

  defp put_implementation(%{updated_at: ts}, ts), do: {:ok, []}

  defp put_implementation(%{id: id} = implementation, _last_updated) do
    [
      ["SADD", "implementation:keys", "implementation:#{id}"],
      ["HMSET", "implementation:#{id}", Map.take(implementation, @props)]
      | structure_id_commands(implementation)
    ]
    |> Redix.transaction_pipeline()
    |> publish_event()
  end

  defp publish_event({:ok, [_, "OK", _, _, [_ | _] = new_structure_ids, _, _]} = res) do
    Publisher.publish(%{
      event: "add_rule_implementation_link",
      stream: "data_structure:events",
      structure_ids: Enum.join(new_structure_ids, ",")
    })

    res
  end

  defp publish_event(res), do: res

  defp delete_implementation(id) do
    Redix.transaction_pipeline([
      ["DEL", "implementation:#{id}"],
      ["DEL", "implementation:#{id}:structure_ids"],
      ["SREM", "implementation:keys", "implementation:#{id}"]
    ])
  end

  defp structure_id_commands(%{id: id, structure_ids: []}) do
    [
      ["DEL", "implementation:#{id}:structure_ids"]
    ]
  end

  defp structure_id_commands(%{id: id, structure_ids: [_ | _] = structure_ids}) do
    [
      ["DEL", "_new_structure_ids"],
      ["SADD", "_new_structure_ids" | structure_ids],
      ["SDIFF", "_new_structure_ids", "implementation:#{id}:structure_ids"],
      ["SINTERSTORE", "implementation:#{id}:structure_ids", "_new_structure_ids"],
      ["DEL", "_new_structure_ids"]
    ]
  end

  defp structure_id_commands(_), do: []
end
