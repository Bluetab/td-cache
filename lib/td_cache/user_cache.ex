defmodule TdCache.UserCache do
  @moduledoc """
  Shared cache for users.
  """
  use GenServer

  alias TdCache.Redix

  @props [:user_name, :full_name, :email, :external_id, :role]

  defmodule Keys do
    @moduledoc false

    def ids, do: "users:ids"

    def group_ids, do: "user_groups:ids"

    def name_to_id, do: "users:name_to_id"

    def user_name_to_id, do: "users:user_name_to_id"

    def external_id_to_id, do: "users:external_id_to_id"

    def user(id), do: "user:#{id}"

    def user_roles(id) do
      "user:#{id}:roles"
    end

    def user_group(id) do
      "user_group:#{id}"
    end

    def user_group_roles(id) do
      "user_group:#{id}:roles"
    end

    def user_resource_roles(user_id, resource_type) do
      "user:#{user_id}:roles:#{resource_type}"
    end
  end

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

  def exists?(user_id) do
    case get(user_id) do
      {:ok, %{id: ^user_id}} -> true
      _ -> false
    end
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

  def get_by_user_name(user_name) do
    GenServer.call(__MODULE__, {:user_name, user_name})
  end

  def get_by_user_name!(user_name) do
    case get_by_user_name(user_name) do
      {:ok, user} -> user
      error -> error
    end
  end

  def get_by_external_id(external_id) do
    GenServer.call(__MODULE__, {:external_id, external_id})
  end

  def get_by_external_id!(external_id) do
    case get_by_external_id(external_id) do
      {:ok, user} -> user
      error -> error
    end
  end

  def get_group(id) do
    GenServer.call(__MODULE__, {:get_group, id})
  end

  def put(user) do
    GenServer.call(__MODULE__, {:put, user})
  end

  def refresh_all_roles(entries) do
    GenServer.call(__MODULE__, {:refresh_all_roles, entries})
  end

  def refresh_resource_roles(user_id, resource_type, entries) do
    GenServer.call(__MODULE__, {:refresh_resource_roles, user_id, resource_type, entries})
  end

  def get_roles(user_id) do
    GenServer.call(__MODULE__, {:get_roles, user_id})
  end

  def get_roles(user_id, resource_type) do
    GenServer.call(__MODULE__, {:get_roles, user_id, resource_type})
  end

  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  def put_group(group) do
    GenServer.call(__MODULE__, {:put_group, group})
  end

  def delete_group(id) do
    GenServer.call(__MODULE__, {:delete_group, id})
  end

  def ids_key, do: Keys.ids()

  def group_ids_key, do: Keys.group_ids()
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

  def handle_call({:get, id}, _from, state) do
    user = get_cache(id, fn -> read_user(id) end)
    {:reply, {:ok, user}, state}
  end

  def handle_call({:name, name}, _from, state) do
    user = read_by_name(name)
    {:reply, {:ok, user}, state}
  end

  def handle_call({:user_name, user_name}, _from, state) do
    user = read_by_user_name(user_name)
    {:reply, {:ok, user}, state}
  end

  def handle_call({:external_id, external_id}, _from, state) do
    user = read_by_external_id(external_id)
    {:reply, {:ok, user}, state}
  end

  def handle_call({:get_group, id}, _from, state) do
    group = read_group(id)
    {:reply, {:ok, group}, state}
  end

  def handle_call({:put, user}, _from, state) do
    reply = put_user(user)
    {:reply, reply, state}
  end

  def handle_call({:refresh_all_roles, entries}, _from, state) do
    reply = do_refresh_all_roles(entries)
    {:reply, reply, state}
  end

  def handle_call({:refresh_resource_roles, user_id, resource_type, entries}, _from, state) do
    reply = do_refresh_resource_roles(user_id, resource_type, entries)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get_roles, user_id}, _from, state) do
    reply = do_get_roles(user_id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get_roles, user_id, resource_type}, _from, state) do
    reply = do_get_roles(user_id, resource_type)
    {:reply, reply, state}
  end

  def handle_call({:delete, id}, _from, state) do
    reply = delete_user(id)
    {:reply, reply, state}
  end

  def handle_call({:put_group, group}, _from, state) do
    reply = do_put_group(group)
    {:reply, reply, state}
  end

  def handle_call({:delete_group, id}, _from, state) do
    reply = do_delete_group(id)
    {:reply, reply, state}
  end

  ## Private functions

  defp get_cache(key, fun) do
    ConCache.get_or_store(:users, key, fn -> fun.() end)
  end

  defp list_users do
    case Redix.command(["SMEMBERS", Keys.ids()]) do
      {:ok, ids} -> Enum.map(ids, &get_cache(&1, fn -> read_user(&1) end))
    end
  end

  defp read_user(id) when is_binary(id) do
    id
    |> String.to_integer()
    |> read_user()
  end

  defp read_user(id) do
    case Redix.read_map(Keys.user(id)) do
      {:ok, nil} -> nil
      {:ok, user} -> Map.put(user, :id, id)
    end
  end

  defp read_by_name(full_name) do
    case Redix.command!(["HGET", Keys.name_to_id(), full_name]) do
      nil -> nil
      id -> read_user(id)
    end
  end

  defp read_by_user_name(user_name) do
    case Redix.command!(["HGET", Keys.user_name_to_id(), user_name]) do
      nil -> nil
      id -> read_user(id)
    end
  end

  defp read_by_external_id(external_id) do
    case Redix.command!(["HGET", Keys.external_id_to_id(), external_id]) do
      nil -> nil
      id -> read_user(id)
    end
  end

  defp read_group(id) when is_binary(id) do
    id
    |> String.to_integer()
    |> read_group()
  end

  defp read_group(id) do
    case Redix.read_map(Keys.user_group(id)) do
      {:ok, nil} -> nil
      {:ok, group} -> Map.put(group, :id, id)
    end
  end

  defp put_user(%{id: id} = user) do
    [
      ["DEL", Keys.user(id)],
      ["HSET", Keys.user(id), get_props(user)],
      ["SADD", Keys.ids(), id]
    ]
    |> add_full_name(user)
    |> add_user_name(user)
    |> add_external_id(user)
    |> Redix.transaction_pipeline()
  end

  defp add_full_name(pipeline, %{id: id, full_name: full_name}) do
    pipeline ++ [["HSET", Keys.name_to_id(), full_name, id]]
  end

  defp add_full_name(pipeline, _), do: pipeline

  defp add_user_name(pipeline, %{id: id, user_name: user_name}) do
    pipeline ++ [["HSET", Keys.user_name_to_id(), user_name, id]]
  end

  defp add_user_name(pipeline, _), do: pipeline

  defp add_external_id(pipeline, %{id: id, external_id: external_id}) when external_id != nil do
    pipeline ++ [["HSET", Keys.external_id_to_id(), external_id, id]]
  end

  defp add_external_id(pipeline, _), do: pipeline

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
    case Redix.command!(["HMGET", Keys.user(id), "full_name", "user_name", "external_id"]) do
      [nil, nil, nil] ->
        Redix.transaction_pipeline([
          ["DEL", Keys.user(id)],
          ["DEL", Keys.user_roles(id)],
          ["SREM", Keys.ids(), id]
        ])

      [full_name, user_name, external_id] ->
        Redix.transaction_pipeline([
          ["DEL", Keys.user(id)],
          ["DEL", Keys.user_roles(id)],
          ["HDEL", Keys.name_to_id(), full_name],
          ["HDEL", Keys.user_name_to_id(), user_name],
          ["HDEL", Keys.external_id_to_id(), external_id],
          ["SREM", Keys.ids(), id]
        ])
    end
  end

  defp build_resources_cmds(resources) do
    Enum.map(resources, fn {resource_type, roles_map} ->
      {resource_type, build_role_args(roles_map)}
    end)
  end

  defp build_role_args(roles_map) do
    Enum.flat_map(roles_map, fn {role, resource_ids} ->
      [role, Enum.join(resource_ids, ",")]
    end)
  end

  defp do_refresh_all_roles(entries) do
    clean_cmd =
      "*"
      |> Keys.user_resource_roles("*")
      |> then(&["KEYS", &1])
      |> Redix.command()
      |> elem(1)
      |> case do
        [_ | _] = keys -> [["DEL" | keys]]
        _ -> []
      end

    hset_cmds =
      Enum.flat_map(entries, fn {user_id, resources} ->
        resources
        |> build_resources_cmds
        |> Enum.map(fn {resource_type, cmd_args} ->
          key = Keys.user_resource_roles(user_id, resource_type)
          ["HSET", key | cmd_args]
        end)
      end)

    case clean_cmd ++ hset_cmds do
      [_ | _] = cmds -> Redix.transaction_pipeline(cmds)
      _ -> {:ok, nil}
    end
  end

  defp do_refresh_resource_roles(user_id, resource_type, entries) do
    key = Keys.user_resource_roles(user_id, resource_type)

    hset_cmd =
      entries
      |> build_role_args()
      |> case do
        [_ | _] = args -> [["HSET", key | args]]
        _ -> []
      end

    Redix.transaction_pipeline([["DEL", key]] ++ hset_cmd)
  end

  defp do_get_roles(user_id, resource_type \\ "domain") do
    key = "user:#{user_id}:roles:#{resource_type}"

    Redix.read_map(key, fn [role, resource_ids] ->
      {role, Redix.to_integer_list!(resource_ids)}
    end)
  end

  defp do_put_group(%{id: id, name: name}) do
    [
      ["DEL", Keys.user_group(id)],
      ["HSET", Keys.user_group(id), %{name: name}],
      ["SADD", Keys.group_ids(), id]
    ]
    |> Redix.transaction_pipeline()
  end

  defp do_delete_group(id) do
    Redix.transaction_pipeline([
      ["DEL", Keys.user_group(id)],
      ["DEL", Keys.user_group_roles(id)],
      ["SREM", Keys.group_ids(), id]
    ])
  end
end
