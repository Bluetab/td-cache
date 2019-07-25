defmodule TdCache.TaxonomyCache do
  @moduledoc """
  Shared cache for taxonomy hierarchy.
  """
  use GenServer

  alias TdCache.DomainCache

  ## Client API

  def start_link(options \\ []) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  def get_domain(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  def get_parent_ids(domain_id, with_self \\ true)

  def get_parent_ids(id, with_self) do
    GenServer.call(__MODULE__, {:parent_ids, id, with_self})
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
  def handle_call({:parent_ids, id, with_self}, _from, state) do
    reply = get_cache({:parent, id}, fn -> do_get_parent_ids(id, with_self) end)
    {:reply, reply, state}
  end

  ## Private functions

  defp get_cache(key, fun) do
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

        domain
        |> Map.put(:parent_ids, parent_ids)
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

  def put_domain(domain) do
    DomainCache.put(domain)
  end

  def delete_domain(domain_id) do
    DomainCache.delete(domain_id)
  end

  @doc """
  Obtain a map of domain names and the corresponding id.

    ## Examples

      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 42, parent_ids: [], name: "Some domain"})
      iex> TaxonomyCache.get_domain_name_to_id_map()
      ...> |> Map.get("Some domain")
      42

  """
  @doc since: "2.8.0"
  def get_domain_name_to_id_map do
    {:ok, map} = DomainCache.name_to_id_map()
    map
  end

  @doc """
  Obtain the set of root domain ids.

    ## Examples

      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 42, parent_ids: [], name: "D1"})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 43, parent_ids: [], name: "D2"})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 44, parent_ids: [1], name: "D3"})
      iex> {:ok, _} = TaxonomyCache.put_domain(%{id: 45, parent_ids: [], name: "D3"})
      iex> root_domain_ids = TaxonomyCache.get_root_domain_ids() |> MapSet.new()
      iex> [42,43,44,45] |> Enum.map(&(MapSet.member?(root_domain_ids, &1)))
      [true, true, false, true]

  """
  @doc since: "2.8.1"
  def get_root_domain_ids do
    {:ok, roots} = DomainCache.roots()
    roots
  end
end
