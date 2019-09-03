defmodule TdCache.ConceptCache do
  @moduledoc """
  Shared cache for data concepts.
  """

  use GenServer

  alias TdCache.DomainCache
  alias TdCache.EventStream.Publisher
  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.RuleCache
  alias TdCache.TaxonomyCache

  require Logger

  @keys "business_concept:keys"
  @active_ids "business_concept:ids:active"
  @inactive_ids "business_concept:ids:inactive"
  @confidential_ids "business_concept:ids:confidential"

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
  def put_active_ids([]), do: {:ok, []}

  def put_active_ids(ids) do
    GenServer.call(__MODULE__, {:ids, ids})
  end

  @doc """
  Updates cache entries for confidential ids.
  """
  def put_confidential_ids(ids) do
    GenServer.call(__MODULE__, {:confidential_ids, ids})
  end

  @doc """
  Reads active concept ids (as a list of strings) from cache.
  """
  def active_ids do
    GenServer.call(__MODULE__, :active_ids)
  end

  @doc """
  Reads confidential concept ids (as a list of strings) from cache.
  """
  def confidential_ids do
    GenServer.call(__MODULE__, :confidential_ids)
  end

  @doc """
  Deletes cache entries relating to a given concept id.
  """
  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  @doc """
  Verifies if id is member of confidential ids set.
  """
  def member_confidential_ids(id) do
    GenServer.call(__MODULE__, {:member, :confidential_ids, id})
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
    concept = get_cache(id, fn -> read_concept(id) end)
    {:reply, {:ok, concept}, state}
  end

  @impl true
  def handle_call({:get, id, :domain_ids}, _from, state) do
    domain_ids =
      case get_cache(id, fn -> read_concept(id) end) do
        %{domain_id: domain_id} ->
          domain_id
          |> String.to_integer()
          |> TaxonomyCache.get_parent_ids()

        _ ->
          []
      end

    {:reply, {:ok, domain_ids}, state}
  end

  @impl true
  def handle_call({:get, id, property}, _from, state) do
    prop =
      case get_cache(id, fn -> read_concept(id) end) do
        nil -> nil
        concept -> Map.get(concept, property)
      end

    {:reply, {:ok, prop}, state}
  end

  @impl true
  def handle_call(:active_ids, _from, state) do
    ids = read_active_ids()
    {:reply, {:ok, ids}, state}
  end

  @impl true
  def handle_call(:confidential_ids, _from, state) do
    ids = read_confidential_ids()
    {:reply, {:ok, ids}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_concept(id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:member, :confidential_ids, id}, _from, state) do
    reply = is_member_confidential_ids?(id)
    {:reply, {:ok, reply}, state}
  end

  @impl true
  def handle_call({:ids, ids}, _from, state) do
    reply = update_active_ids(ids)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:confidential_ids, ids}, _from, state) do
    reply = update_confidential_ids(ids)
    {:reply, reply, state}
  end

  ## Private functions

  @props [:name, :domain_id, :business_concept_version_id, :current_version]
  @confidential "Si"

  defp get_cache(key, fun) do
    ConCache.get_or_store(:concepts, key, fn -> fun.() end)
  end

  defp read_concept(id) do
    concept_key = "business_concept:#{id}"
    {:ok, concept} = Redix.read_map(concept_key)

    case concept_entry_to_map(concept) do
      nil ->
        nil

      m ->
        {:ok, rule_count} = RuleCache.count(concept_key)
        {:ok, link_count} = LinkCache.count(concept_key, "data_structure")

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
      ["SREM", @active_ids, id],
      ["SREM", @confidential_ids, id]
    ]

    results = Redix.transaction_pipeline!(commands)
    [_, _, inactivated, _, _] = results

    unless inactivated == 0 do
      publish_event("remove_concepts", id)
    end

    {:ok, results}
  end

  defp is_member_confidential_ids?(id) do
    ["SISMEMBER", @confidential_ids, id] |> Redix.command!()
  end

  defp put_concept(%{id: id} = concept) do
    commands =
      [
        ["HMSET", "business_concept:#{id}", Map.take(concept, @props)],
        ["SADD", @keys, "business_concept:#{id}"],
        ["SREM", @inactive_ids, id],
        ["SADD", @active_ids, id]
      ]
      |> confidential_ids_command(concept)

    results = Redix.transaction_pipeline!(commands)
    [_, _, activated, _, _] = results

    unless activated == 0 do
      publish_event("restore_concepts", id)
    end

    :ok = ConCache.delete(:concepts, id)

    {:ok, results}
  end

  defp read_active_ids do
    ["SMEMBERS", @active_ids]
    |> Redix.command!()
  end

  def read_confidential_ids do
    ["SMEMBERS", @confidential_ids]
    |> Redix.command!()
  end

  defp update_active_ids(ids) do
    commands = [
      ["RENAME", @active_ids, "_previds"],
      ["RENAME", @inactive_ids, "_prevdeleted"],
      ["SADD", @active_ids] ++ ids,
      ["SINTERSTORE", "_restored", "_prevdeleted", @active_ids],
      ["SDIFFSTORE", "_removed", "_previds", @active_ids],
      ["SDIFFSTORE", @inactive_ids, "_prevdeleted", "_restored"],
      ["SUNIONSTORE", @inactive_ids, @inactive_ids, "_removed"],
      ["SMEMBERS", "_removed"],
      ["SMEMBERS", "_restored"],
      ["DEL", "_previds", "_prevdeleted", "_removed", "_restored"]
    ]

    results = Redix.transaction_pipeline!(commands)
    [_, _, _, _, _, _, _, removed_ids, restored_ids, _] = results
    publish_event("restore_concepts", restored_ids)
    publish_event("remove_concepts", removed_ids)
    {:ok, results}
  end

  defp update_confidential_ids(ids) do
    commands = [
      ["DEL", @confidential_ids],
      ["SADD", @confidential_ids] ++ ids,
      ["SMEMBERS", @confidential_ids]
    ]

    results = Redix.transaction_pipeline!(commands)
    [_, _, confidential_ids] = results
    publish_event("confidential_concepts", confidential_ids)
    {:ok, results}
  end

  defp confidential_ids_command(commands, %{id: id, content: content}) do
    confidential_command =
      case Map.get(content, "_confidential") do
        @confidential -> ["SADD", @confidential_ids, id]
        _ -> ["SREM", @confidential_ids, id]
      end

    commands ++ [confidential_command]
  end

  defp confidential_ids_command(commands, %{id: id}) do
    commands ++ [["SREM", @confidential_ids, id]]
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
        case Redix.transaction_pipeline!(commands) do
          [1 | _] ->
            ids = Redix.command!(["SMEMBERS", @inactive_ids])
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
    |> Enum.filter(fn {from, _} -> Redix.command!(["TYPE", from]) == "set" end)
    |> Enum.map(fn {from, to} -> ["RENAMENX", from, to] end)
  end
end
