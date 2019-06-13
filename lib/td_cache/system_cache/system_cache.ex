defmodule TdCache.SystemCache do
  @moduledoc """
  Shared cache for systems.
  """
  use GenServer
  alias TdCache.Redis

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @doc """
  Creates cache entries relating to a given system.
  """
  def put(system) do
    GenServer.call(__MODULE__, {:put, system})
  end

  @doc """
  Reads system information for a given id from cache.
  """
  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  @doc """
  Deletes cache entries relating to a given system id.
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
  def handle_call({:put, system}, _from, %{conn: conn} = state) do
    reply = put_system(conn, system)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, id}, _from, %{conn: conn} = state) do
    reply = read_system(conn, id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, %{conn: conn} = state) do
    reply = delete_system(conn, id)
    {:reply, reply, state}
  end

  ## Private functions

  defp read_system(conn, id) do
    key = "system:#{id}"
    Redis.read_map(conn, key)
  end

  defp delete_system(conn, id) do
    key = "system:#{id}"
    Redix.command(conn, ["DEL", key])
  end

  defp put_system(conn, %{id: id} = system) do
    key = "system:#{id}"
    Redix.command(conn, Redis.hmset(key, Map.take(system, [:external_id, :name])))
  end
end
