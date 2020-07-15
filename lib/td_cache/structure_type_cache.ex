defmodule TdCache.StructureTypeCache do
  @moduledoc """
  Shared cache for structure types.
  """

  alias TdCache.Redix

  @type_to_id_key "structure_types:type_to_id"

  ## Client API

  @doc """
  Creates cache entries relating to a given structure type.
  """
  def put(structure_type) do
    put_structure_type(structure_type)
  end

  @doc """
  Reads structure type information for a given id from cache.
  """
  def get(id) do
    structure_type = read_structure_type(id)
    {:ok, structure_type}
  end

  def get_by_type(type) do
    structure_type = read_structure_type_by_type(type)
    {:ok, structure_type}
  end

  @doc """
  Deletes cache entries relating to a given structure type id.
  """
  def delete(id) do
    delete_structure_type(id)
  end

  ## Private functions

  @props [:id, :structure_type, :translation, :template_id]

  defp read_structure_type(id) when is_binary(id) do
    id = String.to_integer(id)
    read_structure_type(id)
  end

  defp read_structure_type(id) do
    key = "structure_type:#{id}"
    {:ok, structure_type} = Redix.read_map(key)

    case structure_type do
      nil -> nil
      m -> Map.put(m, :id, id)
    end
  end

  defp read_structure_type_by_type(type) do
    case Redix.command!(["HGET", @type_to_id_key, type]) do
      nil -> nil
      id -> read_structure_type(id)
    end
  end

  defp delete_structure_type(id) do
    key = "structure_type:#{id}"

    case Redix.command!(["HGET", "structure_type:#{id}", :structure_type]) do
      nil ->
        Redix.transaction_pipeline([
          ["DEL", key],
          ["SREM", "structure_type:keys", key]
        ])

      structure_type ->
        Redix.transaction_pipeline([
          ["DEL", key],
          ["SREM", "structure_type:keys", key],
          ["HDEL", @type_to_id_key, structure_type]
        ])
    end
  end

  defp put_structure_type(%{id: id, structure_type: type} = structure_type) do
    key = "structure_type:#{id}"

    Redix.transaction_pipeline([
      ["HMSET", key, Map.take(structure_type, @props)],
      ["SADD", "structure_type:keys", key],
      ["HSET", @type_to_id_key, type, id]
    ])
  end

  defp put_structure_type(_), do: {:error, :empty}
end
