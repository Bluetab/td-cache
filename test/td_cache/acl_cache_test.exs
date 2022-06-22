defmodule TdCache.AclCacheTest do
  use ExUnit.Case

  alias TdCache.AclCache
  alias TdCache.CacheHelpers
  alias TdCache.Redix

  doctest TdCache.AclCache

  @resource_type "test_type"
  @resource_id 987_654_321
  @user_id 987_654_321
  @roles ["role1", "role2", "role3"]
  @role "role1"
  @user_ids [123, 456, 789]
  @group_ids [35, 46, 57]

  setup do
    CacheHelpers.put_user_ids([@user_id | @user_ids])

    on_exit(fn ->
      Redix.del!([
        "acl_roles:test_type:*",
        "acl_role_users:test_type:*",
        "acl_group_roles:test_type:*",
        "acl_role_groups:test_type:*",
        "permission:foo:roles",
        "permission:xxx:roles",
        "user:#{@user_id}:roles"
      ])
    end)
  end

  describe "AclCache.set_acl_roles/3" do
    test "sets members, deletes key if roles is an empty list" do
      assert {:ok, [0, 3]} = AclCache.set_acl_roles(@resource_type, @resource_id, @roles)
      assert Redix.exists?(AclCache.Keys.acl_roles_key(@resource_type, @resource_id))
      assert {:ok, 1} = AclCache.set_acl_roles(@resource_type, @resource_id, [])
      refute Redix.exists?(AclCache.Keys.acl_roles_key(@resource_type, @resource_id))
    end
  end

  test "get_acl_roles returns same value that was put" do
    {:ok, _} = AclCache.set_acl_roles(@resource_type, @resource_id, @roles)
    roles_result = AclCache.get_acl_roles(@resource_type, @resource_id)
    assert Enum.sort(roles_result) == Enum.sort(@roles)
  end

  test "delete_acl_roles deletes from cache" do
    AclCache.set_acl_roles(@resource_type, @resource_id, @roles)
    AclCache.delete_acl_roles(@resource_type, @resource_id)
    key = AclCache.Keys.acl_roles_key(@resource_type, @resource_id)
    refute Redix.exists?(key)
  end

  test "set_acl_role_users accepts an empty list" do
    user_ids = []
    assert {:ok, _} = AclCache.set_acl_role_users(@resource_type, @resource_id, @role, user_ids)
  end

  test "set_acl_role_users returns Ok" do
    role = "role1"

    assert {:ok, [_, 3]} =
             AclCache.set_acl_role_users(@resource_type, @resource_id, role, @user_ids)
  end

  test "get_acl_role_users returns same value that was put" do
    AclCache.set_acl_role_users(@resource_type, @resource_id, @role, @user_ids)
    users_result = AclCache.get_acl_role_users(@resource_type, @resource_id, @role)
    assert Enum.sort(users_result) == @user_ids
  end

  test "get_acl_role_users only includes user ids present in the user:ids key" do
    user_ids = [user_id] = [System.unique_integer([:positive])]
    AclCache.set_acl_role_users(@resource_type, @resource_id, @role, user_ids)

    users_result = AclCache.get_acl_role_users(@resource_type, @resource_id, @role)
    refute user_id in users_result

    CacheHelpers.put_user_ids(user_ids)

    users_result = AclCache.get_acl_role_users(@resource_type, @resource_id, @role)
    assert user_id in users_result
  end

  test "delete_acl_role_users deletes from cache" do
    AclCache.set_acl_role_users(@resource_type, @resource_id, @role, @user_ids)
    AclCache.delete_acl_role_users(@resource_type, @resource_id, @role)
    key = AclCache.Keys.acl_role_users_key(@resource_type, @resource_id, @role)
    refute Redix.exists?(key)
  end

  test "delete_acl_role_user deletes from cache" do
    AclCache.set_acl_role_users(@resource_type, @resource_id, @role, @user_ids)
    AclCache.delete_acl_role_user(@resource_type, @resource_id, @role, Enum.random(@user_ids))
    key = AclCache.Keys.acl_role_users_key(@resource_type, @resource_id, @role)
    {:ok, 2} = Redix.command(["SCARD", key])
  end

  describe "AclCache.set_acl_group_roles/3" do
    test "sets members, deletes key if roles is an empty list" do
      assert {:ok, [0, 3]} = AclCache.set_acl_group_roles(@resource_type, @resource_id, @roles)
      assert Redix.exists?(AclCache.Keys.acl_group_roles_key(@resource_type, @resource_id))
      assert {:ok, 1} = AclCache.set_acl_group_roles(@resource_type, @resource_id, [])
      refute Redix.exists?(AclCache.Keys.acl_group_roles_key(@resource_type, @resource_id))
    end
  end

  test "get_acl_group_roles returns same value that was put" do
    {:ok, _} = AclCache.set_acl_group_roles(@resource_type, @resource_id, @roles)
    roles_result = AclCache.get_acl_group_roles(@resource_type, @resource_id)
    assert Enum.sort(roles_result) == Enum.sort(@roles)
  end

  test "delete_acl_group_roles deletes from cache" do
    AclCache.set_acl_group_roles(@resource_type, @resource_id, @roles)
    AclCache.delete_acl_group_roles(@resource_type, @resource_id)
    key = AclCache.Keys.acl_group_roles_key(@resource_type, @resource_id)
    refute Redix.exists?(key)
  end

  test "set_acl_role_groups accepts an empty list" do
    group_ids = []
    assert {:ok, _} = AclCache.set_acl_role_groups(@resource_type, @resource_id, @role, group_ids)
  end

  test "set_acl_role_groups returns Ok" do
    role = "role1"

    assert {:ok, [_, 3]} =
             AclCache.set_acl_role_users(@resource_type, @resource_id, role, @group_ids)
  end

  test "delete_acl_role_groups deletes from cache" do
    AclCache.set_acl_role_groups(@resource_type, @resource_id, @role, @group_ids)
    AclCache.delete_acl_role_groups(@resource_type, @resource_id, @role)
    key = AclCache.Keys.acl_role_groups_key(@resource_type, @resource_id, @role)
    refute Redix.exists?(key)
  end

  test "delete_acl_role_group deletes from cache" do
    AclCache.set_acl_role_groups(@resource_type, @resource_id, @role, @group_ids)
    AclCache.delete_acl_role_group(@resource_type, @resource_id, @role, Enum.random(@group_ids))
    key = AclCache.Keys.acl_role_groups_key(@resource_type, @resource_id, @role)
    {:ok, 2} = Redix.command(["SCARD", key])
  end

  describe "has_role?/4" do
    test "returns true iff user has role for the resource" do
      AclCache.set_acl_role_users(@resource_type, @resource_id, @role, @user_ids)

      for user_id <- @user_ids do
        assert AclCache.has_role?(@resource_type, @resource_id, @role, user_id)
      end

      refute AclCache.has_role?(@resource_type, @resource_id, @role, 49)
    end
  end
end
