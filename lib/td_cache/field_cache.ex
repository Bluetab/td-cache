defmodule TdCache.FieldCache do
  @moduledoc """
  Shared cache for links between entities.
  """

  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.StructureCache

  require Logger

  # The external ids key is a Set of values "system.group.name.field" written by td-dl
  @external_ids_key "data_fields:external_ids"

  ## Client API

  @doc """
  Creates cache entries relating to a given field.
  """
  def put(field) do
    put_field(field)
  end

  @doc """
  Reads field information from cache.
  """
  def get(id) do
    field = read_field(id)
    {:ok, field}
  end

  @doc """
  Reads field external_id from cache.
  """
  def get_external_id(system, group, name, field) do
    [system, group, name, field]
    |> Enum.join(".")
    |> read_external_id
  end

  @doc """
  Reads field links from cache.
  """
  def links(id) do
    LinkCache.list("data_field", id)
  end

  @doc """
  Deletes cache entries relating to a given field.
  """
  def delete(id) do
    delete_field(id)
  end

  ## Private functions

  defp read_field(id) do
    case Redix.read_map("data_field:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, field} ->
        structure = get_structure(field)

        field
        |> Map.merge(structure)
        |> Map.put(:id, id)
    end
  end

  defp get_structure(%{structure_id: id}) do
    case StructureCache.get(id) do
      {:ok, nil} -> %{}
      {:ok, structure} -> structure
    end
  end

  defp get_structure(_), do: %{}

  defp delete_field(id) do
    key = "data_field:#{id}"

    Redix.transaction_pipeline([
      ["DEL", key],
      ["SREM", "data_field:keys", key]
    ])
  end

  defp put_field(%{
         id: id,
         structure: %{id: structure_id} = structure
       }) do
    field_key = "data_field:#{id}"

    StructureCache.put(structure)

    Redix.transaction_pipeline([
      ["HSET", field_key, "structure_id", structure_id],
      ["SADD", "data_field:keys", field_key]
    ])
  end

  defp put_field(field) do
    Logger.warn("No structure for field #{inspect(field)}")
    {:error, :missing_structure}
  end

  defp read_external_id(value) do
    case Redix.command!(["SISMEMBER", @external_ids_key, value]) do
      1 -> value
      0 -> nil
    end
  end
end
