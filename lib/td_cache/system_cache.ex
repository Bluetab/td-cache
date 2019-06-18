defmodule TdCache.SystemCache do
  @moduledoc """
  Shared cache for systems.
  """
  use GenServer
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands

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
  def init(_args) do
    state = %{}
    {:ok, state}
  end

  @impl true
  def handle_call({:put, system}, _from, state) do
    reply = put_system(system)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    system = read_system(id)
    {:reply, {:ok, system}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_system(id)
    {:reply, reply, state}
  end

  ## Private functions

  @props [:external_id, :name]

  defp read_system(id) when is_binary(id) do
    id = String.to_integer(id)
    read_system(id)
  end

  defp read_system(id) do
    key = "system:#{id}"
    {:ok, system} = Redis.read_map(key)

    case system do
      nil -> nil
      m -> Map.put(m, :id, id)
    end
  end

  defp delete_system(id) do
    key = "system:#{id}"

    Redis.transaction_pipeline([
      ["DEL", key],
      ["SREM", "system:keys", key]
    ])
  end

  defp put_system(%{id: id} = system) do
    key = "system:#{id}"

    Redis.transaction_pipeline([
      Commands.hmset(key, Map.take(system, @props)),
      ["SADD", "system:keys", key]
    ])
  end

  defp put_system(_), do: {:error, :empty}
end
