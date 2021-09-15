defmodule TdCache.DomainCache do
  @moduledoc """
  Shared cache for domains.
  """

  alias TdCache.EventStream.Publisher
  alias TdCache.Redix

  @props [:name, :parent_ids, :external_id, :descendent_ids, :updated_at]
  @roots_key "domains:root"
  @ids_to_names_key "domains:ids_to_names"
  @ids_to_external_ids_key "domains:ids_to_external_ids"
  @domain_keys "domain:keys"
  @deleted_ids "domain:deleted_ids"

  ## Client API

  @doc """
  Creates cache entries relating to a given domain.
  """
  def put(domain) do
    put_domain(domain)
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
  Reads root domains from cache.
  """
  def roots do
    domain_ids = get_root_domains()
    {:ok, domain_ids}
  end

  @doc """
  Reads all domains from cache.
  """
  def domains do
    domain_ids = get_domains()
    {:ok, domain_ids}
  end

  @doc """
  Reads domain external id to id map from cache.
  """
  def external_id_to_id_map do
    map = get_domain_external_id_to_id_map()

    {:ok, map}
  end

  def external_id_to_id(external_id) do
    case get_domain_external_id_to_id_map() do
      %{^external_id => domain_id} -> {:ok, domain_id}
      _ -> :error
    end
  end

  @doc """
  Reads domain name to id map from cache.
  """
  def name_to_id_map do
    map = get_domain_name_to_id_map()

    {:ok, map}
  end

  @doc """
  Deletes cache entries relating to a given domain id.
  """
  def delete(id) do
    delete_domain(id)
  end

  @doc """
  Reads all deleted domains from cache.
  """
  def deleted_domains do
    domain_ids = get_deleted_domains()
    {:ok, domain_ids}
  end

  ## Private functions

  defp read_domain(id) when is_binary(id) do
    id = String.to_integer(id)
    read_domain(id)
  end

  defp read_domain(id) do
    case Redix.read_map("domain:#{id}") do
      {:ok, nil} -> nil
      {:ok, domain} -> Map.put(domain, :id, id)
    end
  end

  defp get_root_domains do
    case Redix.command(["SMEMBERS", @roots_key]) do
      {:ok, ids} -> Enum.map(ids, &String.to_integer/1)
      _ -> []
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

  defp get_domain_name_to_id_map do
    read_map(@ids_to_names_key)
  end

  defp get_domain_external_id_to_id_map do
    read_map(@ids_to_external_ids_key)
  end

  defp delete_domain(id) do
    key = "domain:#{id}"

    commands = [
      ["DEL", key],
      ["HDEL", @ids_to_names_key, id],
      ["HDEL", @ids_to_external_ids_key, id],
      ["SREM", @domain_keys, key],
      ["SREM", @roots_key, id],
      ["SADD", @deleted_ids, id]
    ]

    Redix.transaction_pipeline(commands)
  end

  defp put_domain(%{id: id, updated_at: updated_at} = domain) do
    last_updated = Redix.command!(["HGET", "domain:#{id}", :updated_at])

    domain
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_domain(last_updated)
  end

  defp put_domain(%{updated_at: ts}, ts), do: {:ok, []}

  defp put_domain(%{id: id, name: name} = domain, _ts) do
    parent_ids = domain |> Map.get(:parent_ids, []) |> Enum.join(",")
    descendent_ids = domain |> Map.get(:descendent_ids, []) |> Enum.join(",")
    external_id = Map.get(domain, :external_id)

    domain =
      domain
      |> Map.put(:parent_ids, parent_ids)
      |> Map.put(:descendent_ids, descendent_ids)

    add_or_remove_root = if parent_ids == "", do: "SADD", else: "SREM"

    add_or_remove_external_id =
      case external_id do
        nil -> ["HDEL", @ids_to_external_ids_key, id]
        _ -> ["HSET", @ids_to_external_ids_key, id, external_id]
      end

    commands = [
      ["HMSET", "domain:#{id}", Map.take(domain, @props)],
      ["HSET", @ids_to_names_key, id, name],
      ["SADD", @domain_keys, "domain:#{id}"],
      add_or_remove_external_id,
      [add_or_remove_root, @roots_key, id],
      ["SREM", @deleted_ids, id]
    ]

    {:ok, [_, _, added, _, _, _] = results} = Redix.transaction_pipeline(commands)

    event = %{
      event: if(added == 0, do: "domain_updated", else: "domain_created"),
      domain: "domain:#{id}"
    }

    {:ok, _event_id} = Publisher.publish(event, "domain:events")

    {:ok, results}
  end

  defp put_domain(_, _), do: {:error, :empty}

  defp read_map(collection) do
    case Redix.read_map(collection, fn [id, key] -> {key, String.to_integer(id)} end) do
      {:ok, nil} -> %{}
      {:ok, map} -> map
    end
  end
end
