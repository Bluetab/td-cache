defmodule TdCache.TaxonomyCache do
  @moduledoc """
  Shared cache for taxonomy hierarchy.
  """
  use GenServer

  alias TdCache.DomainCache

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_domain(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  def get_parent_ids(domain_id, with_self \\ true, opts \\ [])

  def get_parent_ids(id, with_self, opts) do
    GenServer.call(__MODULE__, {:parent_ids, id, with_self, opts})
  end

  ## Callbacks

  @impl true
  def init(_options) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    domain = get_cache({:id, id}, fn -> do_get_domain(id) end)
    {:reply, domain, state}
  end

  @impl true
  def handle_call({:parent_ids, id, with_self, opts}, _from, state) do
    reply = get_cache({:parent, id}, fn -> do_get_parent_ids(id, with_self) end, opts[:refresh])
    {:reply, reply, state}
  end

  ## Private functions

  defp get_cache(key, fun, refresh \\ false)

  defp get_cache(key, fun, true) do
    cached = fun.()
    ConCache.put(:taxonomy, key, cached)
    cached
  end

  defp get_cache(key, fun, _refresh) do
    ConCache.get_or_store(:taxonomy, key, fn -> fun.() end)
  end

  defp do_get_domain(domain_id) do
    case DomainCache.get(domain_id) do
      {:ok, nil} ->
        nil

      {:ok, domain} ->
        parent_ids =
          domain
          |> Map.get(:parent_ids)
          |> to_integer_list

        Map.put(domain, :parent_ids, parent_ids)
    end
  end

  defp do_get_parent_ids(domain_id, false) do
    case DomainCache.prop(domain_id, :parent_ids) do
      {:ok, ""} -> []
      {:ok, ids} -> to_integer_list(ids)
    end
  end

  defp do_get_parent_ids(domain_id, true) do
    [domain_id | do_get_parent_ids(domain_id, false)]
  end

  defp to_integer_list(""), do: []

  defp to_integer_list(ids) when is_binary(ids) do
    ids
    |> String.split(",")
    |> Enum.map(&String.to_integer/1)
  end

  defp to_integer_list(_), do: []

  def get_name(domain_id) do
    {:ok, name} = DomainCache.prop(domain_id, :name)
    name
  end

  def put_domain(%{} = domain) do
    delete_local_cache(Map.get(domain, :id))
    DomainCache.put(domain)
  end

  def delete_domain(domain_id) do
    delete_local_cache(domain_id)
    DomainCache.delete(domain_id)
  end

  defp delete_local_cache(id) do
    ConCache.delete(:taxonomy, {:parent, id})
    ConCache.delete(:taxonomy, {:id, id})
  end

  @doc """
  Obtain a map of domain names and the corresponding id.

    ## Examples

      iex> domain = %{id: 42, name: "Some domain", updated_at: DateTime.utc_now()}
      iex> {:ok, _} = TaxonomyCache.put_domain(domain)
      iex> TaxonomyCache.get_domain_name_to_id_map() |> Map.get("Some domain")
      42

  """
  @doc since: "2.8.0"
  def get_domain_name_to_id_map do
    {:ok, map} = DomainCache.name_to_id_map()
    map
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
  Obtain the set of root domain ids.

    ## Examples

      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 42, name: "D1", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 43, name: "D2", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 44, parent_ids: [1], name: "D3", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 45, name: "D3", updated_at: DateTime.utc_now()})
      iex> root_domain_ids = TaxonomyCache.get_root_domain_ids() |> MapSet.new()
      iex> [42,43,44,45] |> Enum.map(&(MapSet.member?(root_domain_ids, &1)))
      [true, true, false, true]

  """
  @doc since: "2.8.1"
  def get_root_domain_ids do
    {:ok, roots} = DomainCache.roots()
    roots
  end

  @doc """
  Obtain the set of domain ids.

    ## Examples

      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 42, name: "D1", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 43, name: "D2", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 44, parent_ids: [1], name: "D3", updated_at: DateTime.utc_now()})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 45, name: "D3", updated_at: DateTime.utc_now()})
      iex> domain_ids = TaxonomyCache.get_domain_ids() |> MapSet.new()
      iex> [42,43,44,45] |> Enum.map(&(MapSet.member?(domain_ids, &1)))
      [true, true, true, true]

  """
  def get_domain_ids do
    {:ok, domains} = DomainCache.domains()
    domains
  end
end
