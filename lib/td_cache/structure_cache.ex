defmodule TdCache.StructureCache do
  @moduledoc """
  Shared cache for data structures.
  """
  use GenServer
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands
  alias TdCache.SystemCache

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @doc """
  Creates cache entries relating to a given structure.
  """
  def put(structure) do
    GenServer.call(__MODULE__, {:put, structure})
  end

  @doc """
  Reads structure information for a given id from cache.
  """
  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  @doc """
  Deletes cache entries relating to a given structure id.
  """
  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  ## Callbacks

  @impl true
  def init(_args) do
    state = %{}
    {:ok, state}
  end

  @impl true
  def handle_call({:put, structure}, _from, state) do
    reply = put_structure(structure)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    structure = read_structure(id)
    {:reply, {:ok, structure}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_structure(id)
    {:reply, reply, state}
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
    structure_key = "data_structure:#{id}"
    structure_path_key = "data_structure:#{id}:path"

    Redis.transaction_pipeline([
      ["DEL", structure_key, structure_path_key],
      ["SREM", "data_structure:keys", structure_key]
    ])
  end

  defp put_structure(structure) do
    commands = structure_commands(structure)

    Redis.transaction_pipeline(commands)
  end

  defp structure_commands(%{id: id} = structure) do
    structure_key = "data_structure:#{id}"

    [
      Commands.hmset(structure_key, Map.take(structure, @props)),
      ["SADD", "data_structure:keys", structure_key]
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
    structure_key = "data_structure:#{id}"

    [
      Commands.hmset(structure_key, ["system_id", "#{system_id}"])
    ]
  end

  defp structure_system_commands(_), do: []
end
