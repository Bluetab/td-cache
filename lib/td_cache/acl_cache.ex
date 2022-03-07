defmodule TdCache.AclCache do
  @moduledoc """
  Shared cache for Access Control Lists.
  """

  alias TdCache.Redix
  alias TdCache.UserCache

  defmodule Keys do
    @moduledoc false

    def acl_roles_key(resource_type, resource_id) do
      "acl_roles:#{resource_type}:#{resource_id}"
    end

    def acl_role_users_key(resource_type, resource_id, role) do
      "acl_role_users:#{resource_type}:#{resource_id}:#{role}"
    end
  end

  def get_acl_roles(resource_type, resource_id) do
    key = Keys.acl_roles_key(resource_type, resource_id)
    {:ok, roles} = Redix.command(["SMEMBERS", key])
    roles
  end

  def set_acl_roles(resource_type, resource_id, roles) when is_list(roles) do
    key = Keys.acl_roles_key(resource_type, resource_id)

    case roles do
      [] -> Redix.command(["DEL", key])
      _ -> Redix.transaction_pipeline([["DEL", key], ["SADD", key | roles]])
    end
  end

  def delete_acl_roles(resource_type, resource_id) do
    key = Keys.acl_roles_key(resource_type, resource_id)
    Redix.command(["DEL", key])
  end

  def get_acl_role_users(resource_type, resource_id, role) do
    key = Keys.acl_role_users_key(resource_type, resource_id, role)

    case Redix.command(["SINTER", key, UserCache.ids_key()]) do
      {:ok, user_ids} -> Redix.to_integer_list!(user_ids)
    end
  end

  def has_role?(resource_type, resource_id, role, user_id) do
    key = Keys.acl_role_users_key(resource_type, resource_id, role)

    Redix.transaction_pipeline!([
      ["SISMEMBER", key, user_id],
      ["SISMEMBER", UserCache.ids_key(), user_id]
    ]) == [1, 1]
  end

  def set_acl_role_users(resource_type, resource_id, role, user_ids) when is_list(user_ids) do
    key = Keys.acl_role_users_key(resource_type, resource_id, role)

    case user_ids do
      [] -> Redix.command(["DEL", key])
      _ -> Redix.transaction_pipeline([["DEL", key], ["SADD", key | user_ids]])
    end
  end

  def delete_acl_role_users(resource_type, resource_id, role) do
    key = Keys.acl_role_users_key(resource_type, resource_id, role)
    Redix.command(["DEL", key])
  end

  def delete_acl_role_user(resource_type, resource_id, role, user_id) do
    key = Keys.acl_role_users_key(resource_type, resource_id, role)
    Redix.command(["SREM", key, user_id])
  end
end
