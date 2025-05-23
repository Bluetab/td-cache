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

    def acl_group_roles_key(resource_type, resource_id) do
      "acl_group_roles:#{resource_type}:#{resource_id}"
    end

    def acl_role_groups_key(resource_type, resource_id, role) do
      "acl_role_groups:#{resource_type}:#{resource_id}:#{role}"
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

  def get_acl_role_resource_domain_ids(resource_type, role) do
    key = Keys.acl_role_users_key(resource_type, "*", role)

    case Redix.command(["KEYS", key]) do
      {:ok, keys} -> Enum.map(keys, fn key -> key |> String.split(":") |> Enum.at(2) end)
    end
  end

  def get_acl_user_ids_by_resources_role(resources, role) do
    keys =
      Enum.flat_map(resources, fn {resource_type, resource_ids} ->
        Enum.map(resource_ids, fn resource_id ->
          Keys.acl_role_users_key(resource_type, resource_id, role)
        end)
      end)

    [
      ["SUNIONSTORE", "union_list"] ++ keys,
      ["SINTER", "union_list", UserCache.ids_key()],
      ["DEL", "union_list"]
    ]
    |> Redix.transaction_pipeline!()
    |> case do
      [_, user_ids, _] -> Redix.to_integer_list!(user_ids)
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

  def get_acl_group_roles(resource_type, resource_id) do
    key = Keys.acl_group_roles_key(resource_type, resource_id)
    {:ok, roles} = Redix.command(["SMEMBERS", key])
    roles
  end

  def set_acl_group_roles(resource_type, resource_id, roles) when is_list(roles) do
    key = Keys.acl_group_roles_key(resource_type, resource_id)

    case roles do
      [] -> Redix.command(["DEL", key])
      _ -> Redix.transaction_pipeline([["DEL", key], ["SADD", key | roles]])
    end
  end

  def delete_acl_group_roles(resource_type, resource_id) do
    key = Keys.acl_group_roles_key(resource_type, resource_id)
    Redix.command(["DEL", key])
  end

  def get_acl_role_groups(resource_type, resource_id, role) do
    key = Keys.acl_role_groups_key(resource_type, resource_id, role)

    case Redix.command(["SINTER", key, UserCache.group_ids_key()]) do
      {:ok, user_group_ids} -> Redix.to_integer_list!(user_group_ids)
    end
  end

  def set_acl_role_groups(resource_type, resource_id, role, user_group_ids)
      when is_list(user_group_ids) do
    key = Keys.acl_role_groups_key(resource_type, resource_id, role)

    case user_group_ids do
      [] -> Redix.command(["DEL", key])
      _ -> Redix.transaction_pipeline([["DEL", key], ["SADD", key | user_group_ids]])
    end
  end

  def delete_acl_role_groups(resource_type, resource_id, role) do
    key = Keys.acl_role_groups_key(resource_type, resource_id, role)
    Redix.command(["DEL", key])
  end

  def delete_acl_role_group(resource_type, resource_id, role, group_id) do
    key = Keys.acl_role_groups_key(resource_type, resource_id, role)
    Redix.command(["SREM", key, group_id])
  end
end
