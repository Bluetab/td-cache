defmodule TdCache.ConceptCache do
  @moduledoc """
  Shared cache for data concepts.
  """

  use GenServer

  alias Jason
  alias TdCache.DomainCache
  alias TdCache.EventStream.Publisher
  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.RuleCache
  alias TdCache.TaxonomyCache

  require Logger

  @active_ids "business_concept:ids:active"
  @confidential_ids "business_concept:ids:confidential"
  @inactive_ids "business_concept:ids:inactive"
  @keys "business_concept:keys"
  @props [
    :name,
    :domain_id,
    :business_concept_version_id,
    :current_version,
    :type,
    :shared_to_ids
  ]

  ## Client API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
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
  def get(id), do: get(id, [])

  @doc """
  Reads concept information for a given id from cache.

  ## Options

    * `refresh` - if `true`, the locally cached value will be refreshed from
      Redis before returning the value
  """
  def get(id, opts) when is_list(opts) do
    GenServer.call(__MODULE__, {:get, id, opts})
  end

  @doc """
  Reads a property of a concept for a given id from cache with options
  """
  def get(id, property, opts \\ []) do
    GenServer.call(__MODULE__, {:get, id, property, opts})
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
  Deletes cache entries relating to a given concept id.
  """
  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  @doc """
  Returns true if the specified id is confidential, false otherwise.
  """
  def is_confidential?(id) do
    case member_confidential_ids(id) do
      {:ok, n} -> n > 0
    end
  end

  @doc """
  Verifies if id is member of confidential ids set.
  """
  def member_confidential_ids(id) do
    GenServer.call(__MODULE__, {:member_confidential_ids, id})
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
  def handle_call({:get, id, opts}, _from, state) do
    concept = get_cache(id, fn -> read_concept(id) end, opts)
    {:reply, {:ok, concept}, state}
  end

  @impl true
  def handle_call({:get, id, :domain_ids, opts}, _from, state) do
    domain_ids =
      case get_cache(id, fn -> read_concept(id) end, opts) do
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
  def handle_call({:get, id, property, opts}, _from, state) do
    prop =
      case get_cache(id, fn -> read_concept(id) end, opts) do
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
  def handle_call({:delete, id}, _from, state) do
    reply = delete_concept(id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:member_confidential_ids, id}, _from, state) do
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

  defp get_cache(key, fun, opts) do
    if Keyword.get(opts, :refresh, false) do
      concept = fun.()
      ConCache.put(:concepts, key, concept)
      concept
    else
      ConCache.get_or_store(:concepts, key, fn -> fun.() end)
    end
  end

  defp read_concept(id) do
    concept_key = "business_concept:#{id}"
    {:ok, concept} = Redix.read_map(concept_key)

    case concept_entry_to_map(concept) do
      nil ->
        nil

      m ->
        {:ok, content} = read_content(concept)
        {:ok, rule_count} = RuleCache.count(concept_key)
        {:ok, link_count} = LinkCache.count(concept_key, "data_structure")
        {:ok, concept_count} = LinkCache.count(concept_key, "business_concept")
        {:ok, shared_to} = read_shared_to(m)

        m
        |> Map.put(:id, id)
        |> Map.put(:rule_count, rule_count)
        |> Map.put(:link_count, link_count)
        |> Map.put(:concept_count, concept_count)
        |> Map.put(:content, content || %{})
        |> Map.put(:shared_to, shared_to)
    end
  end

  defp read_content(%{content: content}) do
    {:ok, Jason.decode!(content)}
  end

  defp read_content(_) do
    {:ok, %{}}
  end

  defp read_shared_to(%{shared_to_ids: shared_to_ids}) do
    {:ok, Enum.map(shared_to_ids, &DomainCache.get!(&1))}
  end

  defp read_shared_to(_) do
    {:ok, []}
  end

  defp concept_entry_to_map(nil), do: nil

  defp concept_entry_to_map(%{} = concept) do
    domain = get_domain(Map.get(concept, :domain_id))
    shared_to_ids = get_shared_to_ids(Map.get(concept, :shared_to_ids))

    concept
    |> Map.put(:domain, domain)
    |> Map.put(:shared_to_ids, shared_to_ids)
  end

  defp get_domain(domain_id) do
    case domain_id do
      nil -> nil
      _ -> DomainCache.get!(domain_id)
    end
  end

  defp get_shared_to_ids(shared_to_ids) do
    case shared_to_ids do
      "" ->
        []

      nil ->
        []

      _ ->
        shared_to_ids
        |> String.split(",")
        |> Enum.map(&String.to_integer/1)
    end
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
    shared_to_ids =
      concept
      |> Map.get(:shared_to_ids, [])
      |> Enum.join(",")

    concept = Map.put(concept, :shared_to_ids, shared_to_ids)

    commands = [
      ["HSET", "business_concept:#{id}", Map.take(concept, @props)],
      [
        "HSET",
        "business_concept:#{id}",
        "content",
        Jason.encode!(Map.get(concept, :content, %{}))
      ],
      ["SADD", @keys, "business_concept:#{id}"],
      ["SREM", @inactive_ids, id],
      ["SADD", @active_ids, id],
      confidential_ids_command(concept)
    ]

    results = Redix.transaction_pipeline!(commands)
    [_, _, _, activated, _, _] = results

    unless activated == 0 do
      publish_event("restore_concepts", id)
    end

    :ok = ConCache.delete(:concepts, id)

    {:ok, results}
  end

  defp read_active_ids do
    ["SMEMBERS", @active_ids]
    |> Redix.command!()
    |> Enum.map(&String.to_integer/1)
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

  defp confidential_ids_command(%{id: id, confidential: confidential}) do
    verb = if confidential, do: "SADD", else: "SREM"
    [verb, @confidential_ids, id]
  end

  defp confidential_ids_command(%{id: id}) do
    ["SREM", @confidential_ids, id]
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
