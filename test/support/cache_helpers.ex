defmodule CacheHelpers do
  @moduledoc """
  Helper functions for creating and cleaning up cache entries for tests.
  """

  alias TdCache.AclCache
  alias TdCache.Redix
  alias TdCache.UserCache

  import ExUnit.Callbacks, only: [on_exit: 1]

  def put_user_ids(user_ids) when is_list(user_ids) do
    key = UserCache.ids_key()
    on_exit(fn -> Redix.command!(["SREM", key | List.wrap(user_ids)]) end)
    Redix.command!(["SADD", key | List.wrap(user_ids)])
  end

  def put_acl(resource_type, resource_id, role, user_ids) do
    on_exit(fn -> AclCache.delete_acl_roles(resource_type, resource_id) end)
    put_user_ids(user_ids)
    AclCache.set_acl_role_users(resource_type, resource_id, role, user_ids)
  end
end
