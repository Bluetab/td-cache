defmodule TdCache.DomainCache do
  @moduledoc """
  Shared cache for domains.
  """
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands

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
  Reads a domain property for a given id from cache.
  """
  def prop(id, property) do
    Redis.command(["HGET", "domain:#{id}", property])
  end

  @doc """
  Reads root domains from cache.
  """
  def roots do
    domain_ids = get_root_domains()
    {:ok, domain_ids}
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

  ## Private functions

  @props [:name, :parent_ids]
  @roots_key "domains:root"
  @ids_to_names_key "domains:ids_to_names"

  defp read_domain(id) when is_binary(id) do
    id = String.to_integer(id)
    read_domain(id)
  end

  defp read_domain(id) do
    case Redis.read_map("domain:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, domain} ->
        domain
        |> Map.put(:id, id)
    end
  end

  defp get_root_domains do
    case Redis.command(["SMEMBERS", @roots_key]) do
      {:ok, ids} -> Enum.map(ids, &String.to_integer/1)
      _ -> []
    end
  end

  defp get_domain_name_to_id_map do
    case Redis.read_map(@ids_to_names_key, fn [id, name] -> {name, String.to_integer(id)} end) do
      {:ok, nil} -> %{}
      {:ok, map} -> map
    end
  end

  defp delete_domain(id) do
    key = "domain:#{id}"

    commands = [
      ["DEL", key],
      ["HDEL", @ids_to_names_key, id],
      ["SREM", "domain:keys", key],
      ["SREM", @roots_key, id]
    ]

    Redis.transaction_pipeline(commands)
  end

  defp put_domain(%{id: id, name: name} = domain) do
    parent_ids = domain |> Map.get(:parent_ids, []) |> Enum.join(",")
    domain = Map.put(domain, :parent_ids, parent_ids)
    add_or_remove_root = if parent_ids == "", do: "SADD", else: "SREM"

    Redis.transaction_pipeline([
      Commands.hmset("domain:#{id}", Map.take(domain, @props)),
      ["HSET", @ids_to_names_key, id, name],
      ["SADD", "domain:keys", "domain:#{id}"],
      [add_or_remove_root, @roots_key, id]
    ])
  end

  defp put_domain(_), do: {:error, :empty}
end
