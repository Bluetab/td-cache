defmodule TdCache.StructureCache do
  @moduledoc """
  Shared cache for data structures.
  """

  alias Jason, as: JSON
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

  ## Private functions

  @props [:name, :type, :group, :system_id, :parent_id, :external_id, :updated_at, :metadata]

  defp read_structure(id) do
    case Redix.read_map("data_structure:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, %{metadata: metadata} = structure} ->
        {:ok, path} = Redix.read_list("data_structure:#{id}:path")
        {:ok, system} = SystemCache.get(Map.get(structure, :system_id))

        structure
        |> put_optional(:path, path)
        |> put_optional(:system, system)
        |> put_optional(:metadata, decode_metadata(metadata))
        |> Map.put(:id, id)
    end
  end

  defp decode_metadata(nil) do
    %{}
  end

  defp decode_metadata(metadata) do
    JSON.decode!(metadata)
  end

  def put_optional(map, _key, nil), do: map
  def put_optional(map, key, value), do: Map.put(map, key, value)

  defp delete_structure(id) do
    Redix.transaction_pipeline([
      ["DEL", "data_structure:#{id}", "data_structure:#{id}:path"],
      ["SREM", "data_structure:keys", "data_structure:#{id}"]
    ])
  end

  defp put_structure(%{id: id, updated_at: updated_at} = structure, opts) do
    last_updated = Redix.command!(["HGET", "data_structure:#{id}", :updated_at])

    structure
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_structure(last_updated, opts[:force])
  end

  defp put_structure(%{updated_at: ts}, ts, false), do: {:ok, []}

  defp put_structure(%{updated_at: ts}, ts, nil), do: {:ok, []}

  defp put_structure(structure, _last_updated, _force) do
    structure
    |> structure_commands()
    |> Redix.transaction_pipeline()
  end

  defp structure_commands(%{id: id} = structure) do
    metadata = Map.get(structure, :metadata)

    structure_props =
      structure
      |> Map.take(@props)
      |> Map.put(:metadata, JSON.encode!(metadata))

    [
      ["HMSET", "data_structure:#{id}", structure_props],
      ["SADD", "data_structure:keys", "data_structure:#{id}"]
    ] ++ structure_path_commands(structure) ++ structure_system_commands(structure)
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

  defp structure_system_commands(%{id: id, system: %{id: system_id}}) do
    [
      ["HMSET", "data_structure:#{id}", "system_id", "#{system_id}"]
    ]
  end

  defp structure_system_commands(_), do: []
end
