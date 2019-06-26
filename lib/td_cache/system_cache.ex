defmodule TdCache.SystemCache do
  @moduledoc """
  Shared cache for systems.
  """

  alias TdCache.Redix

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
      ["SREM", "system:keys", key]
    ])
  end

  defp put_system(%{id: id} = system) do
    key = "system:#{id}"

    Redix.transaction_pipeline([
      ["HMSET", key, Map.take(system, @props)],
      ["SADD", "system:keys", key]
    ])
  end

  defp put_system(_), do: {:error, :empty}
end
