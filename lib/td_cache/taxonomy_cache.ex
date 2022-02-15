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

  def get_domain(id) do
    GenServer.call(__MODULE__, {:get, to_integer(id)})
  end

  def domain_map do
    GenServer.call(__MODULE__, :domain_map)
  end

  def domain_count do
    GenServer.call(__MODULE__, :count)
  end

  def reachable_domain_ids(id_or_ids) when is_integer(id_or_ids) or is_list(id_or_ids) do
    GenServer.call(__MODULE__, {:reachable, id_or_ids})
  end

  def reaching_domain_ids(id_or_ids) when is_integer(id_or_ids) or is_list(id_or_ids) do
    GenServer.call(__MODULE__, {:reaching, id_or_ids})
  end

  def has_role?(domain_id, role, user_id \\ []) do
    GenServer.call(__MODULE__, {:has_role, to_integer(domain_id), role, user_id})
  end

  ## Callbacks

  @impl true
  def init(_options) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    tree = get_cache(:tree, fn -> DomainCache.tree() end)
    domain = get_cache({:id, id, tree}, fn -> do_get_domain(id, tree) end)
    {:reply, domain, state}
  end

  @impl true
  def handle_call(:domain_map, _from, state) do
    tree = get_cache(:tree, fn -> DomainCache.tree() end)
    {:ok, domain_ids} = DomainCache.domains()

    reply =
      domain_ids
      |> Enum.map(&do_get_domain(&1, tree))
      |> Map.new(fn %{id: id} = domain -> {id, domain} end)

    {:reply, reply, state}
  end

  @impl true
  def handle_call(:count, _from, state) do
    tree = get_cache(:tree, fn -> DomainCache.tree() end)
    reply = Graph.no_vertices(tree) - 1

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:reaching, id}, _from, state) do
    tree = get_cache(:tree, fn -> DomainCache.tree() end)
    reply = do_get_reaching_ids(id, tree)

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:reachable, ids}, _from, state) do
    tree = get_cache(:tree, fn -> DomainCache.tree() end)
    reply = do_get_reachable_ids(ids, tree)

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:has_role, domain_id, role, user_id}, _from, state) do
    tree = get_cache(:tree, fn -> DomainCache.tree() end)
    parent_ids = do_get_reaching_ids(domain_id, tree)

    reply =
      get_cache(
        {:has_role, parent_ids, role, user_id},
        fn ->
          Enum.any?(parent_ids, &AclCache.has_role?("domain", &1, role, user_id))
        end
      )

    {:reply, reply, state}
  end

  ## Private functions

  defp get_cache(key, fun) do
    ConCache.get_or_store(:taxonomy, key, fn -> fun.() end)
  end

  defp do_get_domain(domain_id, tree) do
    case DomainCache.get(domain_id) do
      {:ok, nil} ->
        nil

      {:ok, %{id: id} = domain} ->
        parent_ids =
          case do_get_reaching_ids(id, tree) do
            [_ | ids] -> ids
            _ -> []
          end

        domain
        |> Map.put(:parent_id, Enum.at(parent_ids, 0))
        |> Map.put(:parent_ids, parent_ids)
        |> Map.put(:descendent_ids, do_get_reachable_ids(id, tree))
    end
  end

  defp do_get_reaching_ids(domain_id, tree) do
    get_cache(
      {:reaching, domain_id, tree},
      fn ->
        domain_id
        |> List.wrap()
        |> Enum.filter(&Graph.has_vertex?(tree, &1))
        |> Graph.Traversal.reaching(tree)
        |> Enum.reject(&(&1 == 0))
        |> Enum.reverse()
      end
    )
  end

  defp do_get_reachable_ids(domain_id, tree) do
    get_cache(
      {:reachable, domain_id, tree},
      fn ->
        domain_id
        |> List.wrap()
        |> Enum.filter(&Graph.has_vertex?(tree, &1))
        |> Graph.Traversal.reachable(tree)
      end
    )
  end

  defp to_integer(id) when is_integer(id), do: id
  defp to_integer(id) when is_binary(id), do: String.to_integer(id)

  def put_domain(%{} = domain, opts \\ []) do
    domain
    |> get_ids()
    |> delete_local_cache()

    DomainCache.put(domain, opts)
  end

  def delete_domain(domain_id, opts \\ []) do
    delete_local_cache(domain_id)
    DomainCache.delete(domain_id, opts)
  end

  defp delete_local_cache(id_or_ids) do
    tree = ConCache.get(:taxonomy, :tree)
    ConCache.delete(:taxonomy, :tree)

    for id <- List.wrap(id_or_ids) do
      ConCache.delete(:taxonomy, {:id, id, tree})
      ConCache.delete(:taxonomy, {:reaching, id, tree})
      ConCache.delete(:taxonomy, {:reachable, id, tree})
    end
  end

  @doc """
  Obtain a map of domain external ids and the corresponding id.

    ## Examples

      iex> domain = %{id: 42, name: "Some domain", external_id: "External id", updated_at: DateTime.utc_now()}
      iex> {:ok, _} = TaxonomyCache.put_domain(domain)
      iex> TaxonomyCache.get_domain_external_id_to_id_map() |> Map.get("External id")
      42

  """
  def get_domain_external_id_to_id_map do
    {:ok, map} = DomainCache.external_id_to_id_map()
    map
  end

  @doc """
  Obtain the set of domain ids.

    ## Examples

      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 42, name: "D1", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 43, name: "D2", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 44, parent_ids: [1], name: "D3", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 45, name: "D3", updated_at: DateTime.utc_now()})
      iex> domain_ids = TaxonomyCache.get_domain_ids()
      iex> [42,43,44,45] |> Enum.map(&(Enum.member?(domain_ids, &1)))
      [true, true, true, true]

  """
  def get_domain_ids do
    {:ok, domains} = DomainCache.domains()
    domains
  end

  @doc """
  Obtain the set of deleted domain ids.

    ## Examples

      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 42, name: "D1", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 43, name: "D2", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.delete_domain(42)
      iex> {:ok, _} = TaxonomyCache.delete_domain(43)
      iex> domain_ids = TaxonomyCache.get_deleted_domain_ids() |> MapSet.new()
      iex> [42, 43] |> Enum.map(&(MapSet.member?(domain_ids, &1)))
      [true, true]

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
