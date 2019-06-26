defmodule TdCache.StructureCache do
  @moduledoc """
  Shared cache for data structures.
  """
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands
  alias TdCache.SystemCache

  ## Client API

  @doc """
  Creates cache entries relating to a given structure.
  """
  def put(structure) do
    put_structure(structure)
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

  @props [:name, :type, :group, :system_id]

  defp read_structure(id) do
    case Redis.read_map("data_structure:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, structure} ->
        {:ok, path} = Redis.read_list("data_structure:#{id}:path")
        {:ok, system} = SystemCache.get(Map.get(structure, :system_id))

        structure
        |> put_optional(:path, path)
        |> put_optional(:system, system)
        |> Map.put(:id, id)
    end
  end

  def put_optional(map, _key, nil), do: map
  def put_optional(map, key, value), do: Map.put(map, key, value)

  defp delete_structure(id) do
    Redis.transaction_pipeline([
      ["DEL", "data_structure:#{id}", "data_structure:#{id}:path"],
      ["SREM", "data_structure:keys", "data_structure:#{id}"]
    ])
  end

  defp put_structure(structure) do
    commands = structure_commands(structure)

    Redis.transaction_pipeline(commands)
  end

  defp structure_commands(%{id: id} = structure) do
    [
      Commands.hmset("data_structure:#{id}", Map.take(structure, @props)),
      ["SADD", "data_structure:keys", "data_structure:#{id}"]
    ] ++
      structure_path_commands(structure) ++
      structure_system_commands(structure)
  end

  defp structure_path_commands(%{id: id, path: []}) do
    [
      ["DEL", "data_structure:#{id}:path"]
    ]
  end

  defp structure_path_commands(%{id: id, path: path}) do
    [
      ["DEL", "data_structure:#{id}:path"],
      Commands.rpush("data_structure:#{id}:path", path)
    ]
  end

  defp structure_path_commands(_), do: []

  defp structure_system_commands(%{id: id, system: %{id: system_id}}) do
    [
      Commands.hmset("data_structure:#{id}", ["system_id", "#{system_id}"])
    ]
  end

  defp structure_system_commands(_), do: []
end
