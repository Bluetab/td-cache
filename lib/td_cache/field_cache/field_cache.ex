defmodule TdCache.FieldCache do
  @moduledoc """
  Shared cache for links between entities.
  """
  use GenServer

  alias TdCache.Redis
  alias TdCache.StructureCache

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @doc """
  Creates cache entries relating to a given field.
  """
  def put(field) do
    GenServer.call(__MODULE__, {:put, field})
  end

  @doc """
  Reads field information from cache.
  """
  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  @doc """
  Deletes cache entries relating to a given field.
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
  def handle_call({:put, field}, _from, %{conn: conn} = state) do
    reply = put_field(conn, field)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, id}, _from, %{conn: conn} = state) do
    field = read_field(conn, id)
    {:reply, {:ok, field}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, %{conn: conn} = state) do
    reply = delete_field(conn, id)
    {:reply, reply, state}
  end

  ## Private functions

  defp read_field(conn, id) do
    field_key = "field:#{id}"

    {:ok, field} = Redis.read_map(conn, field_key)
    field_entry_to_map(field)
  end

  defp field_entry_to_map(nil), do: nil

  defp field_entry_to_map(field) do
    structure =
      case Map.get(field, :structure_id) do
        nil ->
          nil

        id ->
          {:ok, s} = StructureCache.get(id)
          s
      end

    field
    |> Map.merge(structure)
  end

  defp delete_field(conn, id) do
    key = "field:#{id}"
    Redix.command(conn, ["DEL", key])
  end

  defp put_field(conn, %{
         id: id,
         structure: %{id: structure_id} = structure
       }) do
    field_key = "field:#{id}"

    StructureCache.put(structure)

    commands = [
      ["HMSET", field_key, "structure_id", "#{structure_id}"]
    ]

    Redix.transaction_pipeline(conn, commands)
  end
end
