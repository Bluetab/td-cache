defmodule TdCache.UserCacheTest do
  use ExUnit.Case

  alias TdCache.AclCache
  alias TdCache.Redix
  alias TdCache.UserCache

  doctest TdCache.UserCache

  setup do
    users = Enum.map(0..10, fn _ -> random_user() end)

    {:ok, users: users}
  end

  test "put returns OK", context do
    [user | _] = context[:users]
    assert {:ok, [_, 3, 1, 1, 1]} = put_user(user)
  end

  test "put without full name returns OK", context do
    [user | _] = context[:users]
    user = Map.delete(user, :full_name)
    assert {:ok, [_, 2, 1, 1]} = put_user(user)
  end

  test "put without user name returns OK", context do
    [user | _] = context[:users]
    user = Map.delete(user, :user_name)
    assert {:ok, [_, 2, 1, 1]} = put_user(user)
  end

  test "put without user and full name returns OK", context do
    [user | _] = context[:users]
    user = Map.delete(user, :user_name)
    user = Map.delete(user, :full_name)
    assert {:ok, [_, 1, 1]} = put_user(user)
  end

  test "put without email returns OK", context do
    [user | _] = context[:users]
    user = Map.delete(user, :email)
    assert {:ok, [_, 2, 1, 1, 1]} = put_user(user)
  end

  test "list returns all users", %{users: users} do
    Enum.each(users, &put_user/1)
    {:ok, res} = UserCache.list()
    assert Enum.count(res) == Enum.count(users)
  end

  test "map/0 returns a map of users", %{users: users} do
    Enum.each(users, &put_user/1)
    res = %{} = UserCache.map()
    assert res |> Map.keys() |> Enum.sort() == users |> Enum.map(& &1.id) |> Enum.sort()
  end

  test "get_user returns a map with user_name, full_name and email", context do
    [user | _] = context[:users]
    put_user(user)
    {:ok, u} = UserCache.get(user.id)
    assert u == Map.take(user, [:user_name, :full_name, :email, :id])
  end

  test "get_by_name returns a map with user_name, full_name and email", context do
    [user | _] = context[:users]
    put_user(user)
    {:ok, u} = UserCache.get_by_name(user.full_name)
    assert u == Map.take(user, [:user_name, :full_name, :email, :id])
  end

  test "get_by_user_name returns a map with user_name, full_name and email", context do
    [user | _] = context[:users]
    put_user(user)
    {:ok, u} = UserCache.get_by_user_name(user.user_name)
    assert u == Map.take(user, [:user_name, :full_name, :email, :id])
  end

  test "get_user returns nil if the user is not cached" do
    assert {:ok, nil} == UserCache.get("9876543")
  end

  test "delete_user deletes the user and its ACLs from cache", context do
    [user | _] = context[:users]
    %{
      id: user_id,
      acl_entries: [
        %{
          resource_id: resource_id,
          resource_type: resource_type,
          role: %{
            name: role_name
          }
        }
      ]
    } = user

    put_user(user)
    assert Redix.exists?("user:#{user_id}")
    AclCache.set_acl_role_users(resource_type, resource_id, role_name, [user_id])
    assert AclCache.has_role?(resource_type, resource_id, role_name, user_id)
    UserCache.delete(user)
    assert not Redix.exists?("user:#{user_id}")
    assert not AclCache.has_role?(resource_type, resource_id, role_name, user_id)
  end

  describe "exists?/1" do
    test "returns true if user exists", %{users: [%{id: id1} = user, %{id: id2} | _]} do
      put_user(user)
      assert UserCache.exists?(id1)
      refute UserCache.exists?(id2)
    end
  end

  describe "id_to_email_map/0" do
    test "returns a map with ids as keys and emails as values", %{users: users} do
      for user <- users do
        put_user(user)
      end

      assert %{} = map = UserCache.id_to_email_map()
      assert Enum.count(map) == Enum.count(users)
    end

    test "ignores users without email" do
      random_user()
      |> Map.delete(:email)
      |> put_user()

      assert UserCache.id_to_email_map() == %{}
    end
  end

  describe "put_roles/2 and get_roles/1" do
    test "puts a hash with comma-separated ids as values and reads it back" do
      %{id: user_id} = user = random_user()
      put_user(user)

      domain_ids_by_role = %{
        "role1" => [1, 2, 3],
        "role2" => [4, 5, 6]
      }

      assert {:ok, [0, 2]} = UserCache.put_roles(user_id, domain_ids_by_role)
      assert {:ok, ^domain_ids_by_role} = UserCache.get_roles(user_id)
    end
  end

  defp random_user do
    user_id = System.unique_integer([:positive])
    acl_id = System.unique_integer([:positive])
    resource_id = System.unique_integer([:positive])
    role_id = System.unique_integer([:positive])

    %{
      id: user_id,
      full_name: "user #{user_id}",
      user_name: "user_name#{user_id}",
      email: "user#{user_id}@foo.bar",
      acl_entries: [
        %{
          id: acl_id,
          resource_id: resource_id,
          resource_type: "domain",
          role: %{
            id: role_id,
            name: "role_name"
          }
        }
      ]
    }
  end

  defp put_user(user) do
    on_exit(fn -> UserCache.delete(user) end)
    UserCache.put(user)
  end
end
