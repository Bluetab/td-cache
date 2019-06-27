defmodule TdCache.FieldCache do
  @moduledoc """
  Shared cache for links between entities.
  """

  alias TdCache.EventStream.Publisher
  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.StructureCache

  require Logger

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
  Reads field external_id from cache. The external ids key "data_fields:external_ids"
  is a Set of values "system.group.name.field" written by td-dl.
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

  defp put_field(%{id: id, updated_at: updated_at} = field) do
    last_updated = Redix.command!(["HGET", "data_field:#{id}", :updated_at])

    field
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_field(last_updated)
  end

  defp put_field(field) do
    Logger.warn("No structure for field #{inspect(field)}")
    {:error, :missing_structure}
  end

  defp put_field(%{updated_at: ts}, ts), do: {:ok, []}

  defp put_field(
         %{
           id: id,
           structure: %{id: structure_id} = structure,
           updated_at: updated_at
         },
         _last_updated
       ) do
    StructureCache.put(structure)

    [
      ["HMSET", "data_field:#{id}", "structure_id", structure_id, "updated_at", updated_at],
      ["SADD", "data_field:keys", "data_field:#{id}"]
    ]
    |> Redix.transaction_pipeline()
    |> publish_events(id, structure_id)
  end

  defp publish_events({:ok, ["OK", 0]}, _, _), do: {:ok, ["OK", 0]}

  defp publish_events({:ok, results}, field_id, structure_id) do
    %{
      stream: "data_field:events",
      event: "migrate_field",
      field_id: field_id,
      structure_id: structure_id
    }
    |> Publisher.publish()

    {:ok, results}
  end

  defp read_external_id(value) do
    case Redix.command!(["SISMEMBER", @external_ids_key, value]) do
      1 -> value
      0 -> nil
    end
  end
end
