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
  def handle_call({:delete, id}, _from, state) do
    reply = delete_domain(id)
    {:reply, reply, state}
  end

  ## Private functions

  @props [:name, :parent_id]

  defp read_domain(id) when is_binary(id) do
    id = String.to_integer(id)
    read_domain(id)
  end

  defp read_domain(id) do
    key = "domain:#{id}"
    {:ok, domain} = Redis.read_map(key)

    case domain do
      nil ->
        nil

      d ->
        d =
          case Map.get(d, :parent_id) do
            nil -> d
            parent_id -> Map.put(d, :parent_id, String.to_integer(parent_id))
          end

        case read_parent(d) do
          nil ->
            Map.put(d, :id, id)

          parent ->
            d
            |> Map.put(:id, id)
            |> Map.put(:parent, parent)
        end
    end
  end

  defp read_parent(%{parent_id: parent_id} = _parent), do: read_domain(parent_id)
  defp read_parent(_), do: nil

  defp delete_domain(id) do
    key = "domain:#{id}"

    Redis.transaction_pipeline([
      ["DEL", key],
      ["SREM", "domain:keys", key]
    ])
  end

  defp put_domain(%{id: id} = domain) do
    key = "domain:#{id}"

    Redis.transaction_pipeline([
      Commands.hmset(key, Map.take(domain, @props)),
      ["SADD", "domain:keys", key]
    ])
  end

  defp put_domain(_), do: {:error, :empty}
end
