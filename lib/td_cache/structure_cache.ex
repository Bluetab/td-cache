defmodule TdCache.StructureCache do
  @moduledoc """
  Shared cache for data structures.
  """

  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.SystemCache
  alias TdCache.Utils.MapHelpers

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

  def get_many(ids, opts \\ []) do
    structures =
      ids
      |> Enum.uniq()
      |> read_structures_batch(opts)

    {:ok, structures}
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
    LinkCache.referenced_ids("data_structure:")
  end

  ## Private functions

  @props [
    :name,
    :type,
    :group,
    :system_id,
    :description,
    :parent_id,
    :external_id,
    :updated_at,
    :deleted_at,
    :domain_ids
  ]

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
        |> Map.update(:domain_ids, [], &Redix.to_integer_list!/1)
        |> put_optional(:path, path)
        |> put_optional(:system, system)
        |> Map.put(:metadata, metadata)
        |> Map.put(:id, id)
    end
  end

  defp read_structures_batch(ids, _opts) do
    transform_fun = fn [key, value] -> {String.to_atom(key), value} end

    ids
    |> Enum.map(fn id -> ["HGETALL", "data_structure:#{id}"] end)
    |> Redix.transaction_pipeline()
    |> MapHelpers.zip_results_with_ids(ids)
    |> Enum.map(fn {id, hash} ->
      hash
      |> Redix.hash_to_map(transform_fun)
      |> enrich_structure_map(id)
    end)
  end

  defp enrich_structure_map(structure, id) do
    {:ok, path} = Redix.read_list("data_structure:#{id}:path")
    {:ok, system} = SystemCache.get(Map.get(structure, :system_id))

    metadata =
      case Map.get(structure, :metadata) do
        nil -> %{}
        metadata -> Jason.decode!(metadata)
      end

    structure
    |> Map.update(:domain_ids, [], &Redix.to_integer_list!/1)
    |> put_optional(:path, path)
    |> put_optional(:system, system)
    |> Map.put(:metadata, metadata)
    |> Map.put(:id, id)
  end

  def put_optional(map, _key, nil), do: map
  def put_optional(map, key, value), do: Map.put(map, key, value)

  defp delete_structure(id) do
    Redix.transaction_pipeline([
      ["DEL", "data_structure:#{id}", "data_structure:#{id}:path"],
      ["SREM", "data_structure:keys", "data_structure:#{id}"],
      ["SADD", "data_structure:deleted_ids", id]
    ])
  end

  defp put_structure(%{id: id, updated_at: updated_at, deleted_at: deleted_at} = structure, opts) do
    [last_updated, last_deleted, last_domain_ids] =
      Redix.command!(["HMGET", "data_structure:#{id}", :updated_at, :deleted_at, :domain_ids])

    structure
    |> Map.put(:updated_at, "#{updated_at}")
    |> Map.put(:deleted_at, "#{deleted_at}")
    |> Map.update(:domain_ids, "", &Enum.join(&1, ","))
    |> put_structure(last_updated, last_deleted, last_domain_ids, opts[:force])
  end

  defp put_structure(%{} = structure, opts) do
    structure
    |> Map.put_new(:deleted_at, nil)
    |> Map.put_new(:domain_ids, [])
    |> put_structure(opts)
  end

  defp put_structure(%{updated_at: ts, deleted_at: ds, domain_ids: ids}, ts, ds, ids, false),
    do: {:ok, []}

  defp put_structure(%{updated_at: ts, deleted_at: ds, domain_ids: ids}, ts, ds, ids, nil),
    do: {:ok, []}

  defp put_structure(structure, _last_updated, _last_deleted, _domain_ids, _force) do
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
      ["DEL", "data_structure:#{id}"],
      ["HSET", "data_structure:#{id}", structure_props],
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
