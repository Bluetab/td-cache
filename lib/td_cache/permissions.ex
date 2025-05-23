defmodule TdCache.Permissions do
  @moduledoc """
  Shared cache for permissions.
  """

  alias TdCache.AclCache
  alias TdCache.ConceptCache
  alias TdCache.DomainCache
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
    if default_permission?(permission) do
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

  defp do_has_permission?(session_id, permission, "structure", structure_ids)
       when is_list(structure_ids) do
    case permitted_structure_ids(session_id, permission) do
      [] -> false
      ids -> Enum.any?(structure_ids, &Enum.member?(ids, &1))
    end
  end

  defp do_has_permission?(session_id, permission, "structure", structure_id)
       when is_binary(structure_id) do
    has_permission?(session_id, permission, "domain", String.to_integer(structure_id))
  end

  defp do_has_permission?(session_id, permission, "structure", structure_id)
       when is_integer(structure_id) do
    do_has_permission?(session_id, permission, "structure", [structure_id])
  end

  def has_permission?(session_id, permission, resource_type \\ "domain") do
    if default_permission?(permission) do
      true
    else
      key = session_permissions_key(session_id, resource_type)
      Redix.command!(["HEXISTS", key, permission]) == 1
    end
  end

  def has_any_permission?(session_id, permissions, resource_type, resource_id) do
    Enum.any?(permissions, &has_permission?(session_id, &1, resource_type, resource_id))
  end

  def has_any_permission?(session_id, permissions, resource_type \\ "domain")

  def has_any_permission?(session_id, permissions, resource_type) when is_list(permissions) do
    Enum.any?(permissions, &has_permission?(session_id, &1, resource_type))
  end

  def get_session_permissions(session_id) do
    resource_types =
      session_id
      |> session_permissions_key("*")
      |> Redix.keys!()
      |> Enum.map(&(&1 |> String.split(":") |> Enum.at(-2)))

    Enum.map(resource_types, fn resource_type ->
      key = session_permissions_key(session_id, resource_type)

      case Redix.read_map(key, &transform_function(resource_type, &1)) do
        {:ok, %{} = map} -> {resource_type, map}
        _ -> %{}
      end
    end)
    |> Map.new()
  end

  defp transform_function("domain", [permission, resource_ids]),
    do: {permission, reachable_domain_ids(resource_ids)}

  defp transform_function("structure", [permission, resource_ids]),
    do: {permission, Redix.to_integer_list!(resource_ids)}

  defp reachable_domain_ids(domain_ids) do
    domain_ids
    |> Redix.to_integer_list!()
    |> TaxonomyCache.reachable_domain_ids()
  end

  def cache_session_permissions!(
        session_id,
        expire_at,
        resource_ids_by_type_and_permission
      ) do
    resource_ids_by_type_and_permission
    |> Map.to_list()
    |> Enum.each(fn {resource_type, resource_ids_by_permission} ->
      key = session_permissions_key(session_id, resource_type)

      resource_ids_by_permission
      |> Enum.flat_map(fn {permission, resource_ids} ->
        [permission, Enum.join(resource_ids, ",")]
      end)
      |> do_cache_session_permissions(key, expire_at)
    end)
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

  defp session_permissions_key(session_id, resource_type),
    do: "session:" <> session_id <> ":" <> resource_type <> ":permissions"

  def permitted_domain_ids_by_user_id(user_id, permission) do
    if default_permission?(permission) do
      {:ok, all_domains} = DomainCache.domains()
      all_domains
    else
      {:ok, roles} = get_permission_roles(permission)

      roles
      |> Enum.flat_map(&get_role_domain_pairs/1)
      |> filter_user_roles(user_id)
      |> Enum.map(fn {_role, domain_id} -> domain_id end)
      |> Enum.uniq()
      |> Redix.to_integer_list!()
      |> TaxonomyCache.reachable_domain_ids()
    end
  end

  defp get_role_domain_pairs(role) do
    "domain"
    |> AclCache.get_acl_role_resource_domain_ids(role)
    |> Enum.map(fn domain_id -> {role, domain_id} end)
  end

  defp filter_user_roles(roles_with_domains, user_id) do
    Enum.filter(roles_with_domains, fn {role, domain_id} ->
      AclCache.has_role?("domain", domain_id, role, user_id)
    end)
  end

  def permitted_structure_ids(_session_id, []), do: []

  def permitted_structure_ids(session_id, [_ | _] = permissions) do
    key = session_permissions_key(session_id, "structure")

    ["HMGET", key | permissions]
    |> Redix.command!()
    |> Enum.map(fn structure_ids ->
      structure_ids
      |> Redix.to_integer_list!()
    end)
  end

  def permitted_structure_ids(session_id, permission) do
    session_id
    |> permitted_structure_ids([permission])
    |> hd()
  end

  def permitted_domain_ids(_session_id, []), do: []

  def permitted_domain_ids(session_id, [_ | _] = permissions) do
    with {:ok, cached_default_permissions} <- get_default_permissions(),
         {:ok, all_domains} <- DomainCache.domains() do
      {default_permissions, specific_permissions} =
        Enum.split_with(permissions, &(&1 in cached_default_permissions))

      key = session_permissions_key(session_id, "domain")

      specific_domains =
        ["HMGET", key | specific_permissions]
        |> Redix.command!()
        |> Enum.map(fn domain_ids ->
          domain_ids
          |> Redix.to_integer_list!()
          |> TaxonomyCache.reachable_domain_ids()
        end)

      specific_domains_by_permission = Enum.zip(specific_permissions, specific_domains)
      default_domains_by_permission = Enum.map(default_permissions, &{&1, all_domains})

      permissions_domains_map =
        Map.new(default_domains_by_permission ++ specific_domains_by_permission)

      Enum.map(permissions, &Map.get(permissions_domains_map, &1, []))
    end
  end

  def permitted_domain_ids(session_id, permission) do
    if default_permission?(permission) do
      {:ok, all_domains} = DomainCache.domains()
      all_domains
    else
      session_id
      |> permitted_domain_ids([permission])
      |> hd()
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

  def get_default_permissions do
    Redix.command(["SMEMBERS", @default_permissions_key])
  end

  def default_permission?(permission) do
    Redix.command!(["SISMEMBER", @default_permissions_key, permission]) == 1
  end
end
