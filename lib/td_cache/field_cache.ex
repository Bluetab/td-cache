defmodule TdCache.FieldCache do
  @moduledoc """
  Shared cache for links between entities.
  """
  alias TdCache.ConceptCache
  alias TdCache.LinkCache
  alias TdCache.Redix, as: Redis
  alias TdCache.StructureCache
  require Logger

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
  Deletes cache entries relating to a given field.
  """
  def delete(id) do
    delete_field(id)
  end

  ## Private functions

  defp read_field(id) do
    case Redis.read_map("data_field:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, field} ->
        structure = get_structure(field)
        links = get_links(id)

        field
        |> Map.merge(structure)
        |> Map.put(:links, links)
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

  defp get_links(id) do
    ["SMEMBERS", "data_field:#{id}:links"]
    |> Redis.command!()
    |> Enum.map(&String.replace_prefix(&1, "link:", ""))
    |> Enum.map(&LinkCache.get/1)
    |> Enum.reject(&(&1 == {:ok, nil}))
    |> Enum.map(fn {:ok, %{source: source, tags: tags}} -> {String.split(source, ":"), tags} end)
    |> Enum.map(&read_source/1)
    |> Enum.filter(& &1)
  end

  defp read_source({["business_concept", business_concept_id], tags}) do
    case ConceptCache.get(business_concept_id) do
      {:ok, nil} ->
        nil

      {:ok, concept} ->
        concept
        |> Map.put(:resource_type, :concept)
        |> Map.put(:tags, tags)
    end
  end

  defp read_source(_), do: []

  defp delete_field(id) do
    key = "data_field:#{id}"

    Redis.transaction_pipeline([
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

    Redis.transaction_pipeline([
      ["HSET", field_key, "structure_id", structure_id],
      ["SADD", "data_field:keys", field_key]
    ])
  end

  defp put_field(field) do
    Logger.warn("No structure for field #{inspect(field)}")
    {:error, :missing_structure}
  end
end
