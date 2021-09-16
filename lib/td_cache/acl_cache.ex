defmodule TdCache.AclCache do
  @moduledoc """
  Shared cache for Access Control Lists.
  """

  alias TdCache.Redix

  def create_acl_roles_key(resource_type, resource_id) do
    "acl_roles:#{resource_type}:#{resource_id}"
  end

  def get_acl_roles(resource_type, resource_id) do
    key = create_acl_roles_key(resource_type, resource_id)
    {:ok, roles} = Redix.command(["SMEMBERS", key])
    roles
  end

  def set_acl_roles(resource_type, resource_id, roles) when is_list(roles) do
    key = create_acl_roles_key(resource_type, resource_id)

    Redix.transaction_pipeline([
      ["DEL", key],
      ["SADD", key] ++ roles
    ])
  end

  def set_acl_roles(resource_type, resource_id, %MapSet{} = roles) do
    set_acl_roles(resource_type, resource_id, MapSet.to_list(roles))
  end

  def delete_acl_roles(resource_type, resource_id) do
    key = create_acl_roles_key(resource_type, resource_id)
    Redix.command(["DEL", key])
  end

  def create_acl_role_users_key(resource_type, resource_id, role) do
    "acl_role_users:#{resource_type}:#{resource_id}:#{role}"
  end

  def get_acl_role_users(resource_type, resource_id, role) do
    key = create_acl_role_users_key(resource_type, resource_id, role)
    {:ok, role_users} = Redix.command(["SMEMBERS", key])
    role_users
  end

  def has_role?(resource_type, resource_id, role, user_id) do
    key = create_acl_role_users_key(resource_type, resource_id, role)
    Redix.command!(["SMEMBERS", key])
    Redix.command!(["SISMEMBER", key, user_id]) == 1
  end

  def set_acl_role_users(resource_type, resource_id, role, user_ids) when is_list(user_ids) do
    key = create_acl_role_users_key(resource_type, resource_id, role)

    case user_ids do
      [] ->
        Redix.command(["DEL", key])

      _ ->
        Redix.transaction_pipeline([
          ["DEL", key],
          ["SADD", key] ++ user_ids
        ])
    end
  end

  def delete_acl_role_users(resource_type, resource_id, role) do
    key = create_acl_role_users_key(resource_type, resource_id, role)
    Redix.command(["DEL", key])
  end

  def delete_acl_role_user(resource_type, resource_id, role, user) do
    key = create_acl_role_users_key(resource_type, resource_id, role)
    users = get_acl_role_users(resource_type, resource_id, role)

    case Enum.member?(users, to_string(user)) do
      true -> Redix.command(["SREM", key, "#{user}"])
      _ -> :ok
    end
  end

  def put_role_permissions(roles_by_permission) do
    roles_by_permission
    |> Enum.flat_map(fn {permission, roles} ->
      key = "permission:#{permission}:roles"

      [
        ["DEL", key],
        ["SADD", key | roles]
      ]
    end)
    |> Redix.transaction_pipeline()
  end

  def get_permission_roles(permission) do
    key = "permission:#{permission}:roles"
    Redix.command(["SMEMBERS", key])
  end
end
