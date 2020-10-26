defmodule TdCache.SourceCache do
  @moduledoc """
  Shared cache for CX Sources.
  """

  alias Jason
  alias TdCache.Redix

  ## Client API

  @doc """
  Creates cache entries relating to a given source.
  """
  def put(source) do
    put_source(source)
  end

  @doc """
  Reads source information for a given id from cache.
  """
  def get(id) do
    source = read_source(id)
    {:ok, source}
  end

  @doc """
  Reads all sources from cache.
  """
  def sources do
    source_ids = get_sources()
    {:ok, source_ids}
  end

  @doc """
  Deletes cache entries relating to a given source id.
  """
  def delete(id) do
    delete_source(id)
  end

  @props [:external_id]
  @keys "source:keys"
  @ids_to_external_ids_key "sources:ids_external_ids"

  defp read_source(id) when is_binary(id) do
    id = String.to_integer(id)
    read_source(id)
  end

  defp read_source(id) do
    case Redix.read_map("source:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, %{config: config} = source} ->
        source
        |> Map.put(:id, id)
        |> Map.put(:config, Jason.decode!(config))

      {:ok, source} ->
        Map.put(source, :id, id)
    end
  end

  defp get_sources do
    case Redix.command(["SMEMBERS", @keys]) do
      {:ok, ids} ->
        ids
        |> Enum.map(&read_source_id/1)
        |> Enum.map(&String.to_integer/1)

      _ ->
        []
    end
  end

  defp read_source_id("source:" <> source_id), do: source_id

  defp read_source_id(id), do: id

  defp delete_source(id) do
    key = "source:#{id}"

    Redix.transaction_pipeline([
      ["DEL", key],
      ["SREM", @keys, key],
      ["HDEL", @ids_to_external_ids_key, id]
    ])
  end

  defp put_source(%{id: id, config: config, external_id: external_id} = source) do
    key = "source:#{id}"

    add_or_remove_external_id =
      case external_id do
        nil -> ["HDEL", @ids_to_external_ids_key, id]
        _ -> ["HSET", @ids_to_external_ids_key, id, external_id]
      end

    source =
      source
      |> Map.take(@props)
      |> Map.put(:config, Jason.encode!(config))

    Redix.transaction_pipeline([
      ["HMSET", key, source],
      ["SADD", "source:keys", key],
      add_or_remove_external_id
    ])
  end

  defp put_source(_), do: {:error, :empty}
end
