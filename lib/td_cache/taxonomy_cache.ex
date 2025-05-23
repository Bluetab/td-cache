defmodule TdCache.TaxonomyCache do
  @moduledoc """
  Shared cache for taxonomy hierarchy.
  """
  use GenServer

  alias TdCache.AclCache
  alias TdCache.DomainCache

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec get_domain(binary | integer) :: map | nil
  def get_domain(id) do
    GenServer.call(__MODULE__, {:get, to_integer(id)})
  end

  @spec get_by_external_id(binary) :: map | nil
  def get_by_external_id(external_id) do
    GenServer.call(__MODULE__, {:get_by, external_id})
  end

  def domain_count do
    GenServer.call(__MODULE__, :count)
  end

  def reachable_domain_ids([]), do: []

  def reachable_domain_ids(id_or_ids) when is_integer(id_or_ids) or is_list(id_or_ids) do
    GenServer.call(__MODULE__, {:reachable, id_or_ids})
  end

  def reaching_domain_ids([]), do: []

  def reaching_domain_ids(id_or_ids) when is_integer(id_or_ids) or is_list(id_or_ids) do
    GenServer.call(__MODULE__, {:reaching, id_or_ids})
  end

  def has_role?(domain_id_or_ids, role, user_id)
      when is_integer(domain_id_or_ids) or is_list(domain_id_or_ids) do
    GenServer.call(__MODULE__, {:has_role, domain_id_or_ids, role, user_id})
  end

  ## Callbacks

  @impl true
  def init(_options) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    domain = do_get_domain(id)
    {:reply, domain, state}
  end

  @impl true
  def handle_call({:get_by, external_id}, from, state) do
    case DomainCache.external_id_to_id(external_id) do
      :error -> {:reply, nil, state}
      {:ok, id} -> handle_call({:get, id}, from, state)
    end
  end

  @impl true
  def handle_call(:count, _from, state) do
    count = DomainCache.count!()
    {:reply, count, state}
  end

  @impl true
  def handle_call({:reaching, id}, _from, state) do
    tree = DomainCache.tree()
    reply = do_get_reaching_ids(id, tree)

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:reachable, ids}, _from, state) do
    tree = DomainCache.tree()
    reply = do_get_reachable_ids(ids, tree)

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:has_role, domain_id_or_ids, role, user_id}, _from, state) do
    tree = DomainCache.tree()
    parent_ids = do_get_reaching_ids(domain_id_or_ids, tree)

    reply = Enum.any?(parent_ids, &AclCache.has_role?("domain", &1, role, user_id))

    {:reply, reply, state}
  end

  ## Private functions

  defp do_get_domain(domain_id) do
    case DomainCache.get(domain_id) do
      {:ok, nil} -> nil
      {:ok, domain} -> domain
    end
  end

  defp do_get_reaching_ids(domain_id, tree) do
    domain_id
    |> List.wrap()
    |> Enum.filter(&Graph.has_vertex?(tree, &1))
    |> Graph.Traversal.reaching(tree)
    |> Enum.reject(&(&1 == 0))
    |> Enum.reverse()
  end

  defp do_get_reachable_ids(domain_id, tree) do
    domain_id
    |> List.wrap()
    |> Enum.filter(&Graph.has_vertex?(tree, &1))
    |> Graph.Traversal.reachable(tree)
  end

  defp to_integer(id) when is_integer(id), do: id
  defp to_integer(id) when is_binary(id), do: String.to_integer(id)

  def put_domain(%{} = domain, opts \\ []) do
    ids = get_ids(domain)
    delete_local_cache(ids, Map.get(domain, :external_id))

    DomainCache.put(domain, opts)
  end

  def delete_domain(domain_id, opts \\ []) do
    delete_local_cache(domain_id)
    DomainCache.delete(domain_id, opts)
  end

  defp delete_local_cache(id_or_ids, external_id \\ nil) do
    tree = ConCache.get(:taxonomy, :tree)
    ConCache.delete(:taxonomy, :tree)
    ConCache.delete(:taxonomy, {:external_id, external_id})

    for id <- List.wrap(id_or_ids) do
      ConCache.delete(:taxonomy, {:id, id, tree})
      ConCache.delete(:taxonomy, {:reaching, id, tree})
      ConCache.delete(:taxonomy, {:reachable, id, tree})
    end
  end

  @doc """
  Obtain the set of domain ids.
  """
  def get_domain_ids do
    {:ok, domains} = DomainCache.domains()
    domains
  end

  @doc """
  Obtain the set of deleted domain ids.
  """
  def get_deleted_domain_ids do
    {:ok, domains} = DomainCache.deleted_domains()
    domains
  end

  defp get_ids(%{} = domain) do
    domain
    |> Map.take([:id, :parent_id])
    |> Map.values()
    |> Enum.reject(&is_nil/1)
  end
end
