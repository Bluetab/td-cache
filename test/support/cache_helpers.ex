defmodule TdCache.CacheHelpers do
  @moduledoc """
  Helper functions for creating and cleaning up cache entries for tests.
  """

  alias TdCache.AclCache
  alias TdCache.ConceptCache
  alias TdCache.DomainCache
  alias TdCache.IngestCache
  alias TdCache.Redix
  alias TdCache.UserCache

  import ExUnit.Callbacks, only: [on_exit: 1]

  def put_acl(resource_type, resource_id, role, user_ids) do
    on_exit(fn -> AclCache.delete_acl_roles(resource_type, resource_id) end)
    put_user_ids(user_ids)
    AclCache.set_acl_role_users(resource_type, resource_id, role, user_ids)
  end

  def put_domain(%{id: id} = domain) do
    on_exit(fn -> DomainCache.delete(id) end)
    DomainCache.put(domain, publish: false)
  end

  def put_concept(%{id: id} = concept) do
    on_exit(fn -> ConceptCache.delete(id, publish: false) end)
    ConceptCache.put(concept, publish: false)
  end

  def put_ingest(%{id: id} = ingest) do
    on_exit(fn -> IngestCache.delete(id) end)
    IngestCache.put(ingest)
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
end
