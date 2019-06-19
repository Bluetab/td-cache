defmodule TdCache.DomainCache do
  @moduledoc """
  Shared cache for domains.
  """
  use GenServer
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @doc """
  Creates cache entries relating to a given domain.
  """
  def put(domain) do
    GenServer.call(__MODULE__, {:put, domain})
  end

  @doc """
  Reads domain information for a given id from cache.
  """
  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  @doc """
  Reads root domains from cache.
  """
  def roots do
    GenServer.call(__MODULE__, :roots)
  end

  @doc """
  Reads domain name to id map from cache.
  """
  def name_to_id_map do
    GenServer.call(__MODULE__, :name_to_id_map)
  end

  @doc """
  Reads a domain property for a given id from cache.
  """
  def prop(id, property) do
    GenServer.call(__MODULE__, {:prop, id, property})
  end

  @doc """
  Deletes cache entries relating to a given domain id.
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
  def handle_call({:put, domain}, _from, state) do
    reply = put_domain(domain)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    domain = read_domain(id)
    {:reply, {:ok, domain}, state}
  end

  @impl true
  def handle_call({:prop, id, property}, _from, state) do
    reply = Redis.command(["HGET", "domain:#{id}", property])
    {:reply, reply, state}
  end

  @impl true
  def handle_call(:roots, _from, state) do
    domain_ids = get_root_domains()
    {:reply, {:ok, domain_ids}, state}
  end

  @impl true
  def handle_call(:name_to_id_map, _from, state) do
    map = get_domain_name_to_id_map()

    {:reply, {:ok, map}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_domain(id)
    {:reply, reply, state}
  end

  ## Private functions

  @props [:name, :parent_ids]
  @roots_key "domains:root"

  defp read_domain(id) when is_binary(id) do
    id = String.to_integer(id)
    read_domain(id)
  end

  defp read_domain(id) do
    case Redis.read_map("domain:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, domain} ->
        domain
        |> Map.put(:id, id)
    end
  end

  defp get_root_domains do
    case Redis.command(["SMEMBERS", @roots_key]) do
      {:ok, ids} -> Enum.map(ids, &String.to_integer/1)
      _ -> []
    end
  end

  defp get_domain_name_to_id_map do
    {:ok, keys} = Redis.command(["SMEMBERS", "domain:keys"])

    names =
      keys
      |> Enum.map(&Redis.command(["HGET", &1, "name"]))
      |> Enum.map(fn {:ok, name} -> name end)

    ids = keys |> Enum.map(&id_from_key/1)
    names |> Enum.zip(ids) |> Map.new()
  end

  defp id_from_key("domain:" <> id), do: String.to_integer(id)

  defp delete_domain(id) do
    key = "domain:#{id}"

    Redis.transaction_pipeline([
      ["DEL", key],
      ["SREM", "domain:keys", key],
      ["SREM", @roots_key, id]
    ])
  end

  defp put_domain(%{id: id} = domain) do
    parent_ids = domain |> Map.get(:parent_ids, []) |> Enum.join(",")
    domain = Map.put(domain, :parent_ids, parent_ids)
    add_or_remove_root = if parent_ids == "", do: "SADD", else: "SREM"

    Redis.transaction_pipeline([
      Commands.hmset("domain:#{id}", Map.take(domain, @props)),
      ["SADD", "domain:keys", "domain:#{id}"],
      [add_or_remove_root, @roots_key, id]
    ])
  end

  defp put_domain(_), do: {:error, :empty}
end
