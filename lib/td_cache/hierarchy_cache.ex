defmodule TdCache.HierarchyCache do
  @moduledoc """
  Shared cache for form templates.
  """
  use GenServer

  alias TdCache.EventStream.Publisher
  alias TdCache.Redix

  @node_props [:hierarchy_id, :name, :node_id, :parent_id, :key, :path]
  @hierarchy_props [:name, :id]
  @name_to_id_key "hierarchies:name_to_id"

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  def get(id, prop) do
    case get(id) do
      {:ok, nil} -> {:ok, nil}
      {:ok, hierarchy} -> {:ok, Map.get(hierarchy, prop)}
    end
  end

  def get_node!(key) do
    {:ok, node} = get_node(key)
    node
  end

  def get_node(key) do
    with {:key, [{hierarchy_id, _}, {node_id, _}]} <-
           {:key, key |> String.split("_") |> Enum.map(&Integer.parse(&1, 10))},
         {:ok, nodes} <- get(hierarchy_id, :nodes) do
      {:ok, Enum.find(nodes, &(&1["node_id"] == node_id))}
    else
      {:key, _} -> {:ok, nil}
    end
  end

  def get_by_name(name) do
    GenServer.call(__MODULE__, {:name, name})
  end

  def get_by_name!(name) do
    case get_by_name(name) do
      {:ok, hierarchy} -> hierarchy
      error -> error
    end
  end

  def list do
    GenServer.call(__MODULE__, :list)
  end

  def list! do
    case list() do
      {:ok, hierarchies} -> hierarchies
      error -> error
    end
  end

  @doc """
  Puts or updates a hierarchy in cache. Events may be suppressed by passing the
  option `publish: false`.
  """
  def put(hierarchy, opts \\ []) do
    GenServer.call(__MODULE__, {:put, hierarchy, opts})
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
  def handle_call({:get, id}, _from, state) do
    hierarchy = read_hierarchy(id)
    {:reply, {:ok, hierarchy}, state}
  end

  @impl true
  def handle_call({:name, name}, _from, state) do
    hierarchy = get_cache(name, fn -> read_by_name(name) end)
    {:reply, {:ok, hierarchy}, state}
  end

  @impl true
  def handle_call(:list, _from, state) do
    hierarchies = get_cache(:all, fn -> list_hierarchies() end)
    {:reply, {:ok, hierarchies}, state}
  end

  @impl true
  def handle_call({:put, %{id: id, name: name} = hierarchy, opts}, _from, state) do
    reply = put_hierarchy(hierarchy, opts)

    put_cache(name, read_hierarchy(id))
    put_cache(:all, list_hierarchies())

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    case read_hierarchy(id) do
      nil ->
        :ok

      %{name: name} ->
        delete_cache(name)
    end

    reply = delete_hierarchy(id)

    put_cache(:all, list_hierarchies())

    {:reply, reply, state}
  end

  ## Private functions

  defp get_cache(key, fun) do
    ConCache.isolated(:hierarchies, key, nil, fn ->
      ConCache.get_or_store(:hierarchies, key, fn -> fun.() end)
    end)
  end

  defp put_cache(key, value) do
    ConCache.isolated(:hierarchies, key, nil, fn ->
      ConCache.put(:hierarchies, key, value)
    end)
  end

  defp delete_cache(key) do
    ConCache.isolated(:hierarchies, key, nil, fn ->
      ConCache.delete(:hierarchies, key)
    end)
  end

  defp read_hierarchy(id) when is_binary(id) do
    id
    |> String.to_integer()
    |> read_hierarchy()
  end

  defp read_hierarchy(id) do
    case Redix.read_map("hierarchy:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, %{nodes: nodes} = hierarchy} ->
        hierarchy
        |> Map.put(:id, id)
        |> Map.put(:nodes, Jason.decode!(nodes))

      {:ok, hierarchy} ->
        Map.put(hierarchy, :id, id)
    end
  end

  defp read_by_name("hierarchy:" <> name) do
    read_by_name(name)
  end

  defp read_by_name(name) do
    case Redix.command!(["HGET", @name_to_id_key, name]) do
      nil -> nil
      id -> read_hierarchy(id)
    end
  end

  defp put_hierarchy(
         %{id: id, name: name, nodes: nodes} = hierarchy,
         opts
       ) do
    nodes =
      nodes
      |> Enum.map(fn node ->
        Map.take(node, @node_props)
      end)

    hierarchy =
      hierarchy
      |> Map.take(@hierarchy_props)
      |> Map.put(:nodes, Jason.encode!(nodes))

    commands =
      case prev_names(id, name) do
        [_ | _] = names -> [["HDEL", @name_to_id_key | names]]
        _ -> []
      end

    commands =
      commands ++
        [
          ["HSET", "hierarchy:#{id}", hierarchy],
          ["HSET", @name_to_id_key, name, id],
          ["SADD", "hierarchy:keys", "hierarchy:#{id}"]
        ]

    {:ok, results} = Redix.transaction_pipeline(commands)

    if Keyword.get(opts, :publish, true) do
      event = %{
        event: "hierarchy_updated",
        hierarchy: "hierarchy:#{id}"
      }

      {:ok, _event_id} = Publisher.publish(event, "hierarchy:events")
    end

    {:ok, results}
  end

  defp list_hierarchies do
    case Redix.read_map(@name_to_id_key) do
      {:ok, nil} ->
        []

      {:ok, map} ->
        map
        |> Map.values()
        |> Enum.uniq()
        |> Enum.map(&read_hierarchy/1)
        |> Enum.filter(& &1)
    end
  end

  defp prev_names(id, name) do
    id = to_string(id)

    case Redix.read_map(@name_to_id_key) do
      {:ok, nil} ->
        []

      {:ok, map} ->
        map
        |> Enum.filter(fn {k, v} -> v == id and k != name end)
        |> Enum.map(fn {k, _id} -> k end)
    end
  end

  defp delete_hierarchy(id) do
    case Redix.command!(["HGET", "hierarchy:#{id}", :name]) do
      nil ->
        Redix.transaction_pipeline([
          ["DEL", "hierarchy:#{id}"],
          ["SREM", "hierarchy:keys", "hierarchy:#{id}"]
        ])

      name ->
        Redix.transaction_pipeline([
          ["DEL", "hierarchy:#{id}"],
          ["HDEL", @name_to_id_key, name],
          ["SREM", "hierarchy:keys", "hierarchy:#{id}"]
        ])
    end
  end
end
