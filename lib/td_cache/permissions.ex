defmodule TdCache.Permissions do
  @moduledoc """
  Shared cache for permissions.
  """

  alias TdCache.ConceptCache
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

  def has_permission?(session_id, permission) do
    key = session_permissions_key(session_id)
    Redix.command!(["HEXISTS", key, permission]) == 1
  end

  def has_any_permission?(session_id, permissions, resource_type, resource_id) do
    Enum.any?(permissions, &has_permission?(session_id, &1, resource_type, resource_id))
  end

  def has_any_permission_on_resource_type?(session_id, permissions, "domain") do
    Enum.any?(permissions, &has_permission?(session_id, &1))
  end

  @deprecated "This function will be removed, refactor to avoid using it"
  def get_acls_by_resource_type(session_id, "domain") do
    key = session_permissions_key(session_id)

    case Redix.read_map(key, fn {k, v} -> {k, Redix.to_integer_list!(v)} end) do
      {:ok, map} ->
        map
        |> Enum.flat_map(fn {permission, domain_ids} ->
          Enum.map(domain_ids, &{&1, permission})
        end)
        |> Enum.group_by(fn {domain_id, _} -> domain_id end, fn {_, permission} -> permission end)
        |> Enum.map(fn {domain_id, permissions} ->
          %{
            resource_type: "domain",
            resource_id: String.to_integer(domain_id),
            permissions: permissions
          }
        end)

      _ ->
        []
    end
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
      ["EXPIREAT", key, expire_at]
    ])
  end

  defp session_permissions_key(session_id), do: "session:" <> session_id <> ":permissions"

  def permitted_domain_ids(session_id, permission) do
    key = session_permissions_key(session_id)

    case Redix.command!(["HGET", key, permission]) do
      nil ->
        []

      joined_domain_ids ->
        joined_domain_ids
        |> Redix.to_integer_list!()
        |> TaxonomyCache.reachable_domain_ids()
    end
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

  def is_default_permission?(permission) do
    Redix.command!(["SISMEMBER", @default_permissions_key, permission]) == 1
  end
end
