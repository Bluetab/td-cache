defmodule TdCache.UserCache do
  @moduledoc """
  Shared cache for users.
  """

  use GenServer

  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  def get_by_name(name) do
    GenServer.call(__MODULE__, {:get_by_name, name})
  end

  def get_by_name!(name) do
    case get_by_name(name) do
      {:ok, user} -> user
      error -> error
    end
  end

  def put(user) do
    GenServer.call(__MODULE__, {:put, user})
  end

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
  def handle_call({:get, id}, _from, state) do
    user = read_user(id)
    {:reply, {:ok, user}, state}
  end

  @impl true
  def handle_call({:get_by_name, name}, _from, state) do
    user = read_by_name(name)
    {:reply, {:ok, user}, state}
  end

  @impl true
  def handle_call({:put, user}, _from, state) do
    reply = put_user(user)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_user(id)
    {:reply, reply, state}
  end

  ## Private functions

  @props [:user_name, :full_name, :email]
  @name_to_id_key "users:name_to_id"

  defp read_user(id) when is_binary(id) do
    id
    |> String.to_integer()
    |> read_user
  end

  defp read_user(id) do
    case Redis.read_map("user:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, user} ->
        user
        |> Map.put(:id, id)
    end
  end

  defp read_by_name(full_name) do
    case Redis.command!(["HGET", @name_to_id_key, full_name]) do
      nil -> nil
      id -> read_user(id)
    end
  end

  defp put_user(%{id: id, full_name: full_name} = user) do
    commands = [
      Commands.hmset("user:#{id}", Map.take(user, @props)),
      ["HSET", @name_to_id_key, full_name, id]
    ]

    Redis.transaction_pipeline(commands)
  end

  defp put_user(%{id: id} = user) do
    "user:#{id}"
    |> Commands.hmset(Map.take(user, @props))
    |> Redis.command()
  end

  defp delete_user(id) do
    key = "user:#{id}"

    commands =
      case Redis.command!(["HGET", key, :full_name]) do
        nil ->
          [["DEL", "user:#{id}"]]

        name ->
          [["DEL", "user:#{id}"], ["HDEL", @name_to_id_key, name]]
      end

    Redis.transaction_pipeline(commands)
  end
end
