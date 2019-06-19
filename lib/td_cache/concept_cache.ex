defmodule TdCache.ConceptCache do
  @moduledoc """
  Shared cache for data concepts.
  """
  use GenServer
  alias TdCache.DomainCache
  alias TdCache.LinkCache
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands
  alias TdCache.RuleCache

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @doc """
  Creates cache entries relating to a given concept.
  """
  def put(concept) do
    GenServer.call(__MODULE__, {:put, concept})
  end

  @doc """
  Reads concept information for a given id from cache.
  """
  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  @doc """
  Reads a property of a concept for a given id from cache.
  """
  def get(id, property) do
    GenServer.call(__MODULE__, {:get, id, property})
  end

  @doc """
  Deletes cache entries relating to a given concept id.
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
  def handle_call({:put, concept}, _from, state) do
    reply = put_concept(concept)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    concept = read_concept(id)
    {:reply, {:ok, concept}, state}
  end

  @impl true
  def handle_call({:get, id, property}, _from, state) do
    prop = id |> read_concept |> Map.get(property)
    {:reply, {:ok, prop}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_concept(id)
    {:reply, reply, state}
  end

  ## Private functions

  @props [:id, :name, :domain_id, :business_concept_version_id, :current_version]

  defp read_concept(id) do
    concept_key = "business_concept:#{id}"
    {:ok, concept} = Redis.read_map(concept_key)

    case concept_entry_to_map(concept) do
      nil ->
        nil

      m ->
        {:ok, rule_count} = RuleCache.count(concept_key)
        {:ok, link_count} = LinkCache.count(concept_key, "data_field")

        m
        |> Map.put(:id, id)
        |> Map.put(:rule_count, rule_count)
        |> Map.put(:link_count, link_count)
    end
  end

  def concept_entry_to_map(nil), do: nil

  def concept_entry_to_map(concept) do
    domain =
      case Map.get(concept, :domain_id) do
        nil ->
          nil

        id ->
          {:ok, d} = DomainCache.get(id)
          d
      end

    concept
    |> Map.put(:domain, domain)
  end

  defp delete_concept(id) do
    Redis.transaction_pipeline([
      ["DEL", "business_concept:#{id}"],
      ["SREM", "business_concept:keys", "business_concept:#{id}"]
    ])
  end

  defp put_concept(%{id: id} = concept) do
    Redis.transaction_pipeline([
      Commands.hmset("business_concept:#{id}", Map.take(concept, @props)),
      ["SADD", "business_concept:keys", "business_concept:#{id}"]
    ])
  end
end
