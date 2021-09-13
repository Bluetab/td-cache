defmodule TdCache.AclCacheTest do
  use ExUnit.Case
  alias TdCache.AclCache
  alias TdCache.Redix
  doctest TdCache.AclCache

  @resource_type "test_type"
  @resource_id 987_654_321
  @roles ["role1", "role2", "role3"]
  @user_ids [123, 456, 789]

  setup do
    on_exit(fn ->
      Redix.del!(["acl_roles:test_type:*", "acl_role_users:test_type:*"])
    end)

    :ok
  end

  test "set_acl_roles returns Ok" do
    {:ok, [_, 3]} = AclCache.set_acl_roles(@resource_type, @resource_id, @roles)
  end

  test "get_acl_roles returns same value that was put" do
    roles = MapSet.new(@roles)
    {:ok, _} = AclCache.set_acl_roles(@resource_type, @resource_id, roles)
    roles_result = AclCache.get_acl_roles(@resource_type, @resource_id)
    assert MapSet.new(roles_result) == roles
  end

  test "delete_acl_roles deletes from cache" do
    AclCache.set_acl_roles(@resource_type, @resource_id, @roles)
    AclCache.delete_acl_roles(@resource_type, @resource_id)
    key = AclCache.create_acl_roles_key(@resource_type, @resource_id)
    assert not Redix.exists?("#{key}")
  end

  test "set_acl_role_users accepts an empty list" do
    role = "role1"
    user_ids = []
    assert {:ok, _} = AclCache.set_acl_role_users(@resource_type, @resource_id, role, user_ids)
  end

  test "set_acl_role_users returns Ok" do
    role = "role1"
    assert {:ok, [_, 3]} = AclCache.set_acl_role_users(@resource_type, @resource_id, role, @user_ids)
  end

  test "get_acl_role_users returns same value that was put" do
    role = "role1"
    AclCache.set_acl_role_users(@resource_type, @resource_id, role, @user_ids)
    users_result = AclCache.get_acl_role_users(@resource_type, @resource_id, role)
    assert MapSet.new(users_result, &String.to_integer/1) == MapSet.new(@user_ids)
  end

  test "delete_acl_role_users deletes from cache" do
    role = "role1"
    AclCache.set_acl_role_users(@resource_type, @resource_id, role, @user_ids)
    AclCache.delete_acl_role_users(@resource_type, @resource_id, role)
    key = AclCache.create_acl_role_users_key(@resource_type, @resource_id, role)
    refute Redix.exists?("#{key}")
  end

  test "delete_acl_role_user deletes from cache" do
    role = "role1"
    AclCache.set_acl_role_users(@resource_type, @resource_id, role, @user_ids)
    AclCache.delete_acl_role_user(@resource_type, @resource_id, role, Enum.random(@user_ids))
    key = AclCache.create_acl_role_users_key(@resource_type, @resource_id, role)
    {:ok, 2} = Redix.command(["SCARD", "#{key}"])
  end

  describe "has_role?/4" do
    test "returns true iff user has role for the resource" do
      role = "role1"
      AclCache.set_acl_role_users(@resource_type, @resource_id, role, @user_ids)

      for user_id <- @user_ids do
        assert AclCache.has_role?(@resource_type, @resource_id, role, user_id)
      end

      refute AclCache.has_role?(@resource_type, @resource_id, role, 49)
    end
  end
end
