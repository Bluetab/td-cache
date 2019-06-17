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

  defp read_structure(id) do
    structure_key = "data_structure:#{id}"
    {:ok, structure} = Redis.read_map(structure_key)

    case structure_entry_to_map(id, structure) do
      nil -> nil
      m -> Map.put(m, :id, id)
    end
  end

  def structure_entry_to_map(_, nil), do: nil

  def structure_entry_to_map(id, structure) do
    structure_path_key = "data_structure:#{id}:path"

    system =
      case Map.get(structure, :system_id) do
        nil ->
          nil

        id ->
          {:ok, sys} = SystemCache.get(id)
          sys
      end

    {:ok, path} = Redis.read_list(structure_path_key)

    structure
    |> Map.put(:path, path)
    |> Map.put(:system, system)
    |> Map.drop([:system_id])
  end

  defp delete_structure(id) do
    structure_key = "data_structure:#{id}"
    structure_path_key = "data_structure:#{id}:path"
    Redis.command(["DEL", structure_key, structure_path_key])
  end

  defp put_structure(structure) do
    case Map.get(structure, :system) do
      nil -> :ok
      system -> {:ok, _} = SystemCache.put(system)
    end

    commands = structure_commands(structure)

    Redis.transaction_pipeline(commands)
  end

  defp structure_commands(%{id: id} = structure) do
    structure_key = "data_structure:#{id}"

    [
      Commands.hmset(structure_key, Map.take(structure, [:name, :type, :group]))
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
