defmodule TdCache.CacheHelpers do
  @moduledoc """
  Helper functions for creating and cleaning up cache entries for tests.
  """

  import TdCache.Factory

  alias TdCache.AclCache
  alias TdCache.ConceptCache
  alias TdCache.ImplementationCache
  alias TdCache.IngestCache
  alias TdCache.Permissions
  alias TdCache.Redix
  alias TdCache.TaxonomyCache
  alias TdCache.UserCache

  import ExUnit.Callbacks, only: [on_exit: 1]

  def put_acl(resource_type, resource_id, role, user_ids) do
    on_exit(fn -> AclCache.delete_acl_roles(resource_type, resource_id) end)
    put_user_ids(user_ids)
    AclCache.set_acl_role_users(resource_type, resource_id, role, user_ids)
  end

  def put_domain(%{id: id} = domain) do
    on_exit(fn -> TaxonomyCache.delete_domain(id) end)
    TaxonomyCache.put_domain(domain)
  end

  def put_concept(%{id: id} = concept) do
    on_exit(fn -> ConceptCache.delete(id, publish: false) end)
    ConceptCache.put(concept, publish: false)
  end

  def put_ingest(%{id: id} = ingest) do
    on_exit(fn -> IngestCache.delete(id) end)
    IngestCache.put(ingest)
  end

  def put_implementation(%{id: id} = implementation) do
    on_exit(fn -> ImplementationCache.delete(id) end)
    ImplementationCache.put(implementation)
  end

  def put_user_ids(user_ids) when is_list(user_ids) do
    key = UserCache.ids_key()
    on_exit(fn -> Redix.command!(["SREM", key | List.wrap(user_ids)]) end)
    Redix.command!(["SADD", key | List.wrap(user_ids)])
  end

  def put_user(%{id: id} = user) do
    on_exit(fn -> UserCache.delete(id) end)
    UserCache.put(user)
  end

  def insert_domain do
    domain = build(:domain)
    on_exit(fn -> TaxonomyCache.delete_domain(domain.id, clean: true) end)
    TaxonomyCache.put_domain(domain)
    domain
  end

  def insert_user do
    user = build(:user)
    on_exit(fn -> UserCache.delete(user.id) end)
    UserCache.put(user)
    user
  end

  def insert_group do
    group = build(:group)
    on_exit(fn -> UserCache.delete_group(group.id) end)
    UserCache.put_group(group)
    group
  end

  def put_default_permissions(permissions) do
    on_exit(fn -> Permissions.put_default_permissions([]) end)
    Permissions.put_default_permissions(permissions)
  end
end
