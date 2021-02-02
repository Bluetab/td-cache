defmodule TdCache.StructureCache do
  @moduledoc """
  Shared cache for data structures.
  """

  alias TdCache.ImplementationCache
  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.SystemCache

  ## Client API

  @doc """
  Creates cache entries relating to a given structure.
  """
  def put(structure, opts \\ []) do
    put_structure(structure, opts)
  end

  @doc """
  Reads structure information for a given id from cache.
  """
  def get(id) do
    structure = read_structure(id)
    {:ok, structure}
  end

  @doc """
  Deletes cache entries relating to a given structure id.
  """
  def delete(id) do
    delete_structure(id)
  end

  @doc """
  Returns the ids of referenced structures that have been deleted.
  """
  @spec deleted_ids :: [integer()]
  def deleted_ids do
    ["SMEMBERS", "data_structure:deleted_ids"]
    |> Redix.command!()
    |> Enum.map(&String.to_integer/1)
  end

  @doc """
  Returns a list of structure ids referenced by links or rules
  """
  @spec referenced_ids :: [integer()]
  def referenced_ids do
    rule_structure_ids = ImplementationCache.referenced_structure_ids()
    linked_structure_ids = LinkCache.referenced_ids("data_structure:")

    Enum.uniq(rule_structure_ids ++ linked_structure_ids)
  end

  ## Private functions

  @props [:name, :type, :group, :system_id, :parent_id, :external_id, :updated_at, :deleted_at]

  defp read_structure(id) do
    case Redix.read_map("data_structure:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, structure} ->
        {:ok, path} = Redix.read_list("data_structure:#{id}:path")
        {:ok, system} = SystemCache.get(Map.get(structure, :system_id))

        metadata =
          case Map.get(structure, :metadata) do
            nil -> %{}
            metadata -> Jason.decode!(metadata)
          end

        structure
        |> put_optional(:path, path)
        |> put_optional(:system, system)
        |> Map.put(:metadata, metadata)
        |> Map.put(:id, id)
    end
  end

  def put_optional(map, _key, nil), do: map
  def put_optional(map, key, value), do: Map.put(map, key, value)

  defp delete_structure(id) do
    Redix.transaction_pipeline([
      ["DEL", "data_structure:#{id}", "data_structure:#{id}:path"],
      ["SREM", "data_structure:keys", "data_structure:#{id}"],
      ["SADD", "data_structure:deleted_ids", "#{id}"]
    ])
  end

  defp put_structure(%{id: id, updated_at: updated_at, deleted_at: deleted_at} = structure, opts) do
    [last_updated, last_deleted] =
      Redix.command!(["HMGET", "data_structure:#{id}", :updated_at, :deleted_at])

    structure
    |> Map.put(:updated_at, "#{updated_at}")
    |> Map.put(:deleted_at, "#{deleted_at}")
    |> put_structure(last_updated, last_deleted, opts[:force])
  end

  defp put_structure(%{} = structure, opts) do
    structure
    |> Map.put_new(:deleted_at, nil)
    |> put_structure(opts)
  end

  defp put_structure(%{updated_at: ts, deleted_at: ds}, ts, ds, false), do: {:ok, []}

  defp put_structure(%{updated_at: ts, deleted_at: ds}, ts, ds, nil), do: {:ok, []}

  defp put_structure(structure, _last_updated, _last_deleted, _force) do
    structure
    |> structure_commands()
    |> Redix.transaction_pipeline()
  end

  defp structure_commands(%{id: id} = structure) do
    structure_props =
      structure
      |> Map.take(@props)
      |> add_metadata(structure)

    [
      ["HMSET", "data_structure:#{id}", structure_props],
      ["SADD", "data_structure:keys", "data_structure:#{id}"],
      refresh_deleted_ids_command(structure)
    ] ++ structure_path_commands(structure)
  end

  defp refresh_deleted_ids_command(%{id: id} = structure) do
    case Map.get(structure, :deleted_at) do
      nil -> ["SREM", "data_structure:deleted_ids", id]
      "" -> ["SREM", "data_structure:deleted_ids", id]
      _ -> ["SADD", "data_structure:deleted_ids", id]
    end
  end

  defp structure_path_commands(%{id: id, path: []}) do
    [
      ["DEL", "data_structure:#{id}:path"]
    ]
  end

  defp structure_path_commands(%{id: id, path: path}) do
    [
      ["DEL", "data_structure:#{id}:path"],
      ["RPUSH", "data_structure:#{id}:path", path]
    ]
  end

  defp structure_path_commands(_), do: []

  defp add_metadata(%{} = structure_props, %{metadata: %{} = metadata})
       when map_size(metadata) > 0 do
    Map.put(structure_props, :metadata, Jason.encode!(metadata))
  end

  defp add_metadata(%{} = structure_props, _), do: structure_props
end
