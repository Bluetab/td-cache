defmodule TdCache.StructureCache do
  @moduledoc """
  Shared cache for data structures.
  """
  use GenServer
  alias TdCache.Redis
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
  def init(options) do
    {:ok, conn} = Redix.start_link(host: Keyword.get(options, :redis_host, "redis"))
    state = %{conn: conn}
    {:ok, state}
  end

  @impl true
  def handle_call({:put, structure}, _from, %{conn: conn} = state) do
    reply = put_structure(conn, structure)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, id}, _from, %{conn: conn} = state) do
    structure = read_structure(conn, id)
    {:reply, {:ok, structure}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, %{conn: conn} = state) do
    reply = delete_structure(conn, id)
    {:reply, reply, state}
  end

  ## Private functions

  defp read_structure(conn, id) do
    structure_key = "structure:#{id}"
    {:ok, structure} = Redis.read_map(conn, structure_key)
    structure_entry_to_map(conn, id, structure)
  end

  def structure_entry_to_map(_, _, nil), do: nil

  def structure_entry_to_map(conn, id, structure) do
    structure_path_key = "structure:#{id}:path"
    structure_metadata_key = "structure:#{id}:metadata"

    system =
      case Map.get(structure, :system_id) do
        nil ->
          nil

        id ->
          {:ok, sys} = SystemCache.get(id)
          sys
      end

    {:ok, path} = Redis.read_list(conn, structure_path_key)
    {:ok, meta} = Redis.read_map(conn, structure_metadata_key)

    structure
    |> Map.put(:path, path)
    |> Map.put(:system, system)
    |> Map.put(:metadata, meta)
    |> Map.drop([:system_id])
  end

  defp delete_structure(conn, id) do
    structure_key = "structure:#{id}"
    structure_path_key = "structure:#{id}:path"
    structure_metadata_key = "structure:#{id}:metadata"
    Redix.command(conn, ["DEL", structure_key, structure_path_key, structure_metadata_key])
  end

  defp put_structure(conn, structure) do
    case Map.get(structure, :system) do
      nil -> :ok
      system -> {:ok, _} = SystemCache.put(system)
    end

    commands = structure_commands(structure)

    Redix.transaction_pipeline(conn, commands)
  end

  defp structure_commands(%{id: id} = structure) do
    structure_key = "structure:#{id}"

    [
      Redis.hmset(structure_key, Map.take(structure, [:name, :type, :group]))
    ] ++
      structure_path_commands(structure) ++
      structure_metadata_commands(structure) ++
      structure_system_commands(structure)
  end

  defp structure_path_commands(%{id: id, path: path}) do
    structure_path = "structure:#{id}:path"

    [
      ["DEL", structure_path],
      Redis.rpush(structure_path, path)
    ]
  end

  defp structure_path_commands(_), do: []

  defp structure_system_commands(%{id: id, system: %{id: system_id}}) do
    structure_key = "structure:#{id}"

    [
      Redis.hmset(structure_key, ["system_id", "#{system_id}"])
    ]
  end

  defp structure_system_commands(_), do: []

  defp structure_metadata_commands(%{id: id, metadata: %{} = metadata}) when metadata != %{} do
    metadata_key = "structure:#{id}:metadata"

    [
      Redis.hmset(metadata_key, metadata)
    ]
  end

  defp structure_metadata_commands(_), do: []
end
