defmodule TdCache.DomainCache do
  @moduledoc """
  Shared cache for domains.
  """

  alias TdCache.Redix

  @props [:name, :external_id, :updated_at, :parent_id]
  @ids_to_names_key "domains:ids_to_names"
  @ids_to_external_ids_key "domains:ids_to_external_ids"
  @graph_key "domains:graph"
  @domain_keys "domain:keys"
  @deleted_ids "domain:deleted_ids"
  @root_id 0

  ## Client API

  @doc """
  Returns the tree of domain ids, including an artifical vertex 0 as the root
  """
  def tree do
    ["HGETALL", @graph_key]
    |> Redix.command!()
    |> Enum.map(&to_id/1)
    |> create_graph()
  end

  @doc """
  Creates cache entries relating to a given domain.
  """
  def put(domain, opts \\ []) do
    put_domain(domain, opts)
  end

  @doc """
  Reads domain information for a given id from cache.
  """
  def get(id) do
    domain = read_domain(id)
    {:ok, domain}
  end

  @doc """
  Reads domain information for a given id from cache.
  """
  def get!(id) do
    read_domain(id)
  end

  @doc """
  Reads a domain property for a given id from cache.
  """
  def prop(id, property) do
    Redix.command(["HGET", "domain:#{id}", property])
  end

  @doc """
  Reads all domain ids from cache.
  """
  def domains do
    domain_ids = get_domains()
    {:ok, domain_ids}
  end

  @doc """
  Reads count of domains from cache.
  """
  def count! do
    Redix.command!(["SCARD", @domain_keys])
  end

  @doc """
  Reads domain external id to id map from cache.
  """
  def external_id_to_id_map do
    map = read_map(@ids_to_external_ids_key, &id_vals/1)

    {:ok, map}
  end

  @doc """
  Retuns a map of domains where the key is the id and the value is the name.
  """
  def id_to_name_map do
    map = read_map(@ids_to_names_key, &id_keys/1)

    {:ok, map}
  end

  @doc """
  Retuns a map of domains where the key is the id and the value is the external_id.
  """
  def id_to_external_id_map do
    map =
      @ids_to_external_ids_key
      |> read_map(&id_vals/1)
      |> Enum.map(fn {key, value} -> {value, key} end)
      |> Map.new()

    {:ok, map}
  end

  def external_id_to_id(external_id) do
    case external_id_to_id_map() do
      {:ok, %{^external_id => domain_id}} -> {:ok, domain_id}
      _ -> :error
    end
  end

  @doc """
  Deletes cache entries relating to a given domain id.
  """
  def delete(id, opts \\ []) do
    delete_domain(id, opts)
  end

  @doc """
  Reads all deleted domains from cache.
  """
  def deleted_domains do
    domain_ids = get_deleted_domains()
    {:ok, domain_ids}
  end

  ## Private functions

  @spec read_domain(integer | binary) :: nil | map
  defp read_domain(id) do
    case Redix.read_map("domain:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, domain} ->
        domain
        |> Map.put(:id, to_id(id))
        |> Map.update(:parent_id, nil, &to_id/1)
    end
  end

  defp get_domains do
    case Redix.command(["SMEMBERS", @domain_keys]) do
      {:ok, ids} ->
        ids
        |> Enum.map(&read_domain_id/1)
        |> Enum.map(&String.to_integer/1)

      _ ->
        []
    end
  end

  defp get_deleted_domains do
    case Redix.command(["SMEMBERS", @deleted_ids]) do
      {:ok, ids} -> Enum.map(ids, &String.to_integer/1)
      _ -> []
    end
  end

  defp read_domain_id("domain:" <> domain_id), do: domain_id

  defp read_domain_id(id), do: id

  defp delete_domain(id, opts) do
    key = "domain:#{id}"

    add_or_remove = if Keyword.get(opts, :clean, false), do: "SREM", else: "SADD"

    commands = [
      ["DEL", key],
      ["HDEL", @graph_key, id],
      ["HDEL", @ids_to_names_key, id],
      ["HDEL", @ids_to_external_ids_key, id],
      ["SREM", @domain_keys, key],
      [add_or_remove, @deleted_ids, id]
    ]

    Redix.transaction_pipeline(commands)
  end

  defp put_domain(%{id: id, updated_at: updated_at} = domain, opts) do
    last_updated = Redix.command!(["HGET", "domain:#{id}", :updated_at])

    domain
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_domain(last_updated, opts[:force])
  end

  defp put_domain(_, _), do: {:error, :invalid}

  defp put_domain(%{updated_at: ts}, ts, false), do: {:ok, []}

  defp put_domain(%{updated_at: ts}, ts, nil), do: {:ok, []}

  defp put_domain(%{id: id, name: name} = domain, _ts, _force)
       when is_integer(id) and id != @root_id do
    parent_id = Map.get(domain, :parent_id) || @root_id
    external_id = Map.get(domain, :external_id)

    add_or_remove_external_id =
      case external_id do
        nil -> ["HDEL", @ids_to_external_ids_key, id]
        _ -> ["HSET", @ids_to_external_ids_key, id, external_id]
      end

    commands = [
      ["DEL", "domain:#{id}"],
      ["HSET", "domain:#{id}", Map.take(domain, @props)],
      ["HSET", @ids_to_names_key, id, name],
      ["SADD", @domain_keys, "domain:#{id}"],
      add_or_remove_external_id,
      ["HSET", @graph_key, id, parent_id],
      ["SREM", @deleted_ids, id]
    ]

    Redix.transaction_pipeline(commands)
  end

  defp put_domain(_, _, _), do: {:error, :invalid}

  defp read_map(collection, transform) do
    case Redix.read_map(collection, transform) do
      {:ok, nil} -> %{}
      {:ok, map} -> map
    end
  end

  defp to_id(id) when is_integer(id), do: id
  defp to_id(""), do: nil
  defp to_id(id) when is_binary(id), do: String.to_integer(id)

  defp create_graph(entries) do
    create_graph(Graph.new([], acyclic: true), entries)
  end

  defp create_graph(graph, []), do: graph

  defp create_graph(graph, [child_id, child_id | entries]) do
    graph
    |> Graph.add_vertex(child_id)
    |> create_graph(entries)
  end

  defp create_graph(graph, [child_id, parent_id | entries]) do
    graph
    |> Graph.add_vertex(child_id)
    |> Graph.add_vertex(parent_id)
    |> Graph.add_edge(parent_id, child_id)
    |> create_graph(entries)
  end

  defp id_vals([id, key]), do: {key, String.to_integer(id)}
  defp id_keys([id, val]), do: {String.to_integer(id), val}
end
