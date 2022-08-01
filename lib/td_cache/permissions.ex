defmodule TdCache.Permissions do
  @moduledoc """
  Shared cache for permissions.
  """

  alias TdCache.ConceptCache
  alias TdCache.ImplementationCache
  alias TdCache.IngestCache
  alias TdCache.Redix
  alias TdCache.TaxonomyCache

  @default_permissions_key "permission:defaults"

  def has_permission?(session_id, permission, resource_type, resource_id)

  def has_permission?(session_id, permission, resource_type, resource_id)
      when is_atom(permission) do
    has_permission?(session_id, Atom.to_string(permission), resource_type, resource_id)
  end

  def has_permission?(session_id, permission, resource_type, resource_id) do
    if is_default_permission?(permission) do
      true
    else
      do_has_permission?(session_id, permission, resource_type, resource_id)
    end
  end

  defp do_has_permission?(session_id, permission, "domain", domain_ids)
       when is_list(domain_ids) do
    case permitted_domain_ids(session_id, permission) do
      [] -> false
      ids -> Enum.any?(domain_ids, &Enum.member?(ids, &1))
    end
  end

  defp do_has_permission?(session_id, permission, "domain", domain_id)
       when is_binary(domain_id) do
    has_permission?(session_id, permission, "domain", String.to_integer(domain_id))
  end

  defp do_has_permission?(session_id, permission, "domain", domain_id) do
    case permitted_domain_ids(session_id, permission) do
      [] -> false
      ids -> Enum.member?(ids, domain_id)
    end
  end

  defp do_has_permission?(session_id, permission, "business_concept", business_concept_id) do
    {:ok, domain_id} = ConceptCache.get(business_concept_id, :domain_id)
    has_permission?(session_id, permission, "domain", domain_id)
  end

  defp do_has_permission?(session_id, permission, "ingest", ingest_id) do
    domain_id = IngestCache.get_domain_id(ingest_id)
    has_permission?(session_id, permission, "domain", domain_id)
  end

  defp do_has_permission?(session_id, permission, "implementation", implementation_id) do
    domain_id = ImplementationCache.get_domain_id(implementation_id)
    has_permission?(session_id, permission, "domain", domain_id)
  end

  def has_permission?(session_id, permission) do
    if is_default_permission?(permission) do
      true
    else
      key = session_permissions_key(session_id)
      Redix.command!(["HEXISTS", key, permission]) == 1
    end
  end

  def has_any_permission?(session_id, permissions, resource_type, resource_id) do
    Enum.any?(permissions, &has_permission?(session_id, &1, resource_type, resource_id))
  end

  def has_any_permission?(session_id, permissions) when is_list(permissions) do
    Enum.any?(permissions, &has_permission?(session_id, &1))
  end

  def get_session_permissions(session_id) do
    key = session_permissions_key(session_id)

    case Redix.read_map(key, fn [k, v] -> {k, reachable_domain_ids(v)} end) do
      {:ok, %{} = map} -> map
      _ -> %{}
    end
  end

  defp reachable_domain_ids(domain_ids) do
    domain_ids
    |> Redix.to_integer_list!()
    |> TaxonomyCache.reachable_domain_ids()
  end

  def cache_session_permissions!(session_id, expire_at, domain_ids_by_permission) do
    key = session_permissions_key(session_id)

    domain_ids_by_permission
    |> Enum.flat_map(fn {permission, domain_ids} -> [permission, Enum.join(domain_ids, ",")] end)
    |> do_cache_session_permissions(key, expire_at)
  end

  defp do_cache_session_permissions([], key, _), do: Redix.command!(["DEL", key])

  defp do_cache_session_permissions(entries, key, expire_at) do
    Redix.transaction_pipeline!([
      ["DEL", key],
      ["HSET", key | entries],
      expire_cmd(key, expire_at)
    ])
  end

  defp expire_cmd(key, nil), do: ["EXPIRE", key, 1]
  defp expire_cmd(key, expire_at), do: ["EXPIREAT", key, expire_at]

  defp session_permissions_key(session_id), do: "session:" <> session_id <> ":permissions"

  def permitted_domain_ids(_session_id, []), do: [[]]

  def permitted_domain_ids(session_id, [_|_] = permissions) do
    key = session_permissions_key(session_id)

    (["HMGET", key] ++ permissions)
    |> Redix.command!()
    |> Enum.map(fn domain_ids_by_permission ->
      case domain_ids_by_permission do
        nil ->
          []

        domain_ids ->
          domain_ids
          |> Redix.to_integer_list!()
          |> TaxonomyCache.reachable_domain_ids()
      end
    end)
  end

  def permitted_domain_ids(session_id, permission) do
    [domain_ids] = permitted_domain_ids(session_id, [permission])
    domain_ids
  end

  def put_permission_roles(roles_by_permission) do
    deletes =
      ["KEYS", "permission:*:roles"]
      |> Redix.command!()
      |> Enum.map(&["DEL", &1])

    adds =
      Enum.map(roles_by_permission, fn {permission, roles} ->
        key = "permission:#{permission}:roles"
        ["SADD", key | roles]
      end)

    Redix.transaction_pipeline(deletes ++ adds)
  end

  def get_permission_roles(permission) do
    key = "permission:#{permission}:roles"
    Redix.command(["SMEMBERS", key])
  end

  def put_default_permissions([]) do
    Redix.command(["DEL", @default_permissions_key])
  end

  def put_default_permissions(permissions) do
    Redix.transaction_pipeline([
      ["DEL", @default_permissions_key],
      ["SADD", @default_permissions_key | permissions]
    ])
  end

  def get_default_permissions do
    Redix.command(["SMEMBERS", @default_permissions_key])
  end

  def is_default_permission?(permission) do
    Redix.command!(["SISMEMBER", @default_permissions_key, permission]) == 1
  end
end
