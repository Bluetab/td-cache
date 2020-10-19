defmodule TdCache.SystemCache do
  @moduledoc """
  Shared cache for systems.
  """

  alias TdCache.Redix

  @ids_to_external_ids_key "systems:ids_external_ids"

  ## Client API

  @doc """
  Creates cache entries relating to a given system.
  """
  def put(system) do
    put_system(system)
  end

  @doc """
  Reads system information for a given id from cache.
  """
  def get(id) do
    system = read_system(id)
    {:ok, system}
  end

  @doc """
  Deletes cache entries relating to a given system id.
  """
  def delete(id) do
    delete_system(id)
  end

  @doc """
  Reads external ids to id map from cache.
  """
  def external_id_to_id_map do
    map = get_system_external_id_to_id_map()

    {:ok, map}
  end

  ## Private functions

  @props [:external_id, :name]

  defp read_system(id) when is_binary(id) do
    id = String.to_integer(id)
    read_system(id)
  end

  defp read_system(id) do
    key = "system:#{id}"
    {:ok, system} = Redix.read_map(key)

    case system do
      nil -> nil
      m -> Map.put(m, :id, id)
    end
  end

  defp delete_system(id) do
    key = "system:#{id}"

    Redix.transaction_pipeline([
      ["DEL", key],
      ["SREM", "system:keys", key],
      ["HDEL", @ids_to_external_ids_key, id]
    ])
  end

  defp put_system(%{id: id} = system) do
    key = "system:#{id}"
    external_id = Map.get(system, :external_id)

    add_or_remove_external_id =
      case external_id do
        nil -> ["HDEL", @ids_to_external_ids_key, id]
        _ -> ["HSET", @ids_to_external_ids_key, id, external_id]
      end

    Redix.transaction_pipeline([
      ["HMSET", key, Map.take(system, @props)],
      ["SADD", "system:keys", key],
      add_or_remove_external_id
    ])
  end

  defp put_system(_), do: {:error, :empty}

  defp get_system_external_id_to_id_map do
    read_map(@ids_to_external_ids_key)
  end

  defp read_map(collection) do
    case Redix.read_map(collection, fn [id, key] -> {key, String.to_integer(id)} end) do
      {:ok, nil} -> %{}
      {:ok, map} -> map
    end
  end
end
