defmodule TdCache.ConceptCache do
  @moduledoc """
  Shared cache for data concepts.
  """
  use GenServer
  alias TdCache.DomainCache
  alias TdCache.EventStream.Publisher
  alias TdCache.LinkCache
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands
  alias TdCache.RuleCache

  require Logger

  @keys "business_concept:keys"
  @active_ids "business_concept:ids:active"
  @inactive_ids "business_concept:ids:inactive"

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
  Updates cache entries for active and inactive (deleted/deprecated) ids.
  Events will be emitted for newly inactivated ids.
  """
  def put_active_ids(ids) do
    GenServer.call(__MODULE__, {:ids, ids})
  end

  @doc """
  Reads active concept ids (as a list of strings) from cache.
  """
  def active_ids do
    GenServer.call(__MODULE__, :active_ids)
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
    Process.send_after(self(), :migrate, 0)
    {:ok, state}
  end

  @impl true
  def handle_info(:migrate, state) do
    migrate()
    {:noreply, state}
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
  def handle_call(:active_ids, _from, state) do
    ids = read_active_ids()
    {:reply, {:ok, ids}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_concept(id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:ids, ids}, _from, state) do
    reply = update_active_ids(ids)
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
    commands = [
      ["DEL", "business_concept:#{id}"],
      ["SREM", @keys, "business_concept:#{id}"],
      ["SADD", @inactive_ids, id],
      ["SREM", @active_ids, id]
    ]

    results = Redis.transaction_pipeline!(commands)
    [_, _, inactivated, _] = results

    unless inactivated == 0 do
      publish_event("remove_concepts", id)
    end

    {:ok, results}
  end

  defp put_concept(%{id: id} = concept) do
    commands = [
      Commands.hmset("business_concept:#{id}", Map.take(concept, @props)),
      ["SADD", @keys, "business_concept:#{id}"],
      ["SREM", @inactive_ids, id],
      ["SADD", @active_ids, id]
    ]

    results = Redis.transaction_pipeline!(commands)
    [_, _, activated, _] = results

    unless activated == 0 do
      publish_event("restore_concepts", id)
    end

    {:ok, results}
  end

  defp read_active_ids do
    ["SMEMBERS", @active_ids]
    |> Redis.command!()
  end

  defp update_active_ids(ids) do
    commands = [
      ["RENAME", @active_ids, "_previds"],
      ["RENAME", @inactive_ids, "_prevdeleted"],
      ["SADD", "_ids"] ++ ids,
      ["SINTERSTORE", "_restored", "_prevdeleted", "_ids"],
      ["SDIFFSTORE", "_removed", "_previds", "_ids"],
      ["SDIFFSTORE", "_deleted", "_prevdeleted", "_restored"],
      ["SUNIONSTORE", "_deleted", "_deleted", "_removed"],
      ["RENAME", "_ids", @active_ids],
      ["RENAME", "_deleted", @inactive_ids],
      ["SMEMBERS", "_removed"],
      ["SMEMBERS", "_restored"],
      ["DEL", "_previds", "_prevdeleted", "_removed", "_restored"]
    ]

    results = Redis.transaction_pipeline!(commands)
    [_, _, _, _, _, _, _, _, _, removed_ids, restored_ids, _] = results
    publish_event("restore_concepts", restored_ids)
    publish_event("remove_concepts", removed_ids)
    {:ok, results}
  end

  defp publish_event(_, []), do: :ok

  defp publish_event(event, ids) when is_list(ids) do
    ids = Enum.join(ids, ",")
    publish_event(event, ids)
  end

  defp publish_event(event, ids) do
    %{
      stream: "business_concept:events",
      event: event,
      ids: ids
    }
    |> Publisher.publish()
  end

  defp migrate do
    commands = migration_commands()
    case commands do
      [] ->
        :ok

      _ ->
        case Redis.transaction_pipeline!(commands) do
          [1 | _] ->
            ids = Redis.command!(["SMEMBERS", @inactive_ids])
            publish_event("remove_concepts", ids)
            count = Enum.count(ids)
            Logger.info("Migrated active/inactive concept keys (#{count}} deprecated ids)")

          _ ->
            :ok
        end
    end
  end

  defp migration_commands do
    %{
      "deprecated_business_concepts" => @inactive_ids,
      "existing_business_concepts" => @active_ids
    }
    |> Enum.filter(fn {from, _} -> Redis.command!(["TYPE", from]) == "set" end)
    |> Enum.map(fn {from, to} -> ["RENAMENX", from, to] end)
  end
end
