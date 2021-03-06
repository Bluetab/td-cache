defmodule TdCache.UserCache do
  @moduledoc """
  Shared cache for users.
  """
  use GenServer

  alias TdCache.Redix

  @ids "users:ids"
  @props [:user_name, :full_name, :email]
  @name_to_id_key "users:name_to_id"

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def list do
    GenServer.call(__MODULE__, :list)
  end

  @doc """
  Returns a map of cached users with user ids as keys and users as values.
  """
  def map do
    with {:ok, users} <- list() do
      Map.new(users, fn %{id: id} = user -> {id, user} end)
    end
  end

  def id_to_email_map do
    with {:ok, users} <- list() do
      users
      |> Enum.flat_map(fn
        %{id: id, email: email} when is_binary(email) -> [{id, email}]
        _ -> []
      end)
      |> Map.new()
    end
  end

  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  def get_by_name(name) do
    GenServer.call(__MODULE__, {:name, name})
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
  def init(_options) do
    {:ok, nil}
  end

  @impl true
  def handle_call(:list, _from, state) do
    users = list_users()
    {:reply, {:ok, users}, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    user = get_cache(id, fn -> read_user(id) end)
    {:reply, {:ok, user}, state}
  end

  @impl true
  def handle_call({:name, name}, _from, state) do
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

  defp get_cache(key, fun) do
    ConCache.get_or_store(:users, key, fn -> fun.() end)
  end

  defp list_users do
    case Redix.command(["SMEMBERS", @ids]) do
      {:ok, ids} -> Enum.map(ids, &get_cache(&1, fn -> read_user(&1) end))
    end
  end

  defp read_user(id) when is_binary(id) do
    id
    |> String.to_integer()
    |> read_user
  end

  defp read_user(id) do
    case Redix.read_map("user:#{id}") do
      {:ok, nil} -> nil
      {:ok, user} -> Map.put(user, :id, id)
    end
  end

  defp read_by_name(full_name) do
    case Redix.command!(["HGET", @name_to_id_key, full_name]) do
      nil -> nil
      id -> read_user(id)
    end
  end

  defp put_user(%{id: id, full_name: full_name} = user) do
    Redix.transaction_pipeline([
      ["DEL", "user:#{id}"],
      ["HMSET", "user:#{id}", get_props(user)],
      ["SADD", @ids, "#{id}"],
      ["HSET", @name_to_id_key, full_name, id]
    ])
  end

  defp put_user(%{id: id} = user) do
    Redix.transaction_pipeline([
      ["DEL", "user:#{id}"],
      ["HMSET", "user:#{id}", get_props(user)],
      ["SADD", @ids, "#{id}"]
    ])
  end

  defp get_props(%{} = user) do
    user
    |> Map.take(@props)
    |> Enum.reject(fn
      {:email, nil} -> true
      {:email, ""} -> true
      _ -> false
    end)
    |> Map.new()
  end

  defp delete_user(id) do
    case Redix.command!(["HGET", "user:#{id}", :full_name]) do
      nil ->
        Redix.transaction_pipeline([
          ["DEL", "user:#{id}"],
          ["SREM", @ids, "#{id}"]
        ])

      name ->
        Redix.transaction_pipeline([
          ["DEL", "user:#{id}"],
          ["HDEL", @name_to_id_key, name],
          ["SREM", @ids, "#{id}"]
        ])
    end
  end
end
