defmodule TdCache.UserCacheTest do
  use ExUnit.Case

  import TdCache.Factory

  alias TdCache.Redix
  alias TdCache.UserCache

  doctest TdCache.UserCache

  setup do
    # Clean up all user_roles
    on_exit(fn -> UserCache.refresh_all_roles(%{}) end)
    users = Enum.map(0..10, fn _ -> build(:user) end)
    [users: users, user: Enum.random(users)]
  end

  test "put returns OK", %{user: user} do
    assert {:ok, [_, 4, 1, 1, 1, 1]} = put_user(user)
  end

  test "put without full name returns OK", %{user: user} do
    user = Map.delete(user, :full_name)
    assert {:ok, [_, 3, 1, 1, 1]} = put_user(user)
  end

  test "put without user name returns OK", %{user: user} do
    user = Map.delete(user, :user_name)
    assert {:ok, [_, 3, 1, 1, 1]} = put_user(user)
  end

  test "put without external_id returns OK", %{user: user} do
    user = Map.delete(user, :external_id)
    assert {:ok, [_, 3, 1, 1, 1]} = put_user(user)
  end

  test "put without user name and full name returns OK", %{user: user} do
    user = Map.drop(user, [:user_name, :full_name])
    assert {:ok, [_, 2, 1, 1]} = put_user(user)
  end

  test "put without user name, full name and external_id returns OK", %{user: user} do
    user = Map.drop(user, [:user_name, :full_name, :external_id])
    assert {:ok, [_, 1, 1]} = put_user(user)
  end

  test "put without email returns OK", %{user: user} do
    user = Map.delete(user, :email)
    assert {:ok, [_, 3, 1, 1, 1, 1]} = put_user(user)
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

  test "get_user returns a map with user_name, full_name, email and external_id", %{user: user} do
    put_user(user)
    {:ok, u} = UserCache.get(user.id)
    assert u == Map.take(user, [:user_name, :full_name, :email, :id, :external_id])
  end

  test "get_by_name returns a map with user_name, full_name, email and external_id", %{user: user} do
    put_user(user)
    {:ok, u} = UserCache.get_by_name(user.full_name)
    assert u == Map.take(user, [:user_name, :full_name, :email, :id, :external_id])
  end

  test "get_by_user_name returns a map with user_name, full_name, email and external_id", %{
    user: user
  } do
    put_user(user)
    {:ok, u} = UserCache.get_by_user_name(user.user_name)
    assert u == Map.take(user, [:user_name, :full_name, :email, :id, :external_id])
  end

  test "get_user returns nil if the user is not cached" do
    assert {:ok, nil} == UserCache.get(System.unique_integer([:positive]))
  end

  describe "delete/1" do
    test "deletes the user from cache", %{user: %{id: user_id} = user} do
      put_user(user)
      assert Redix.exists?("user:#{user_id}")
      UserCache.delete(user_id)
      refute Redix.exists?("user:#{user_id}")
    end
  end

  describe "exists?/1" do
    test "returns true if user exists", %{user: %{id: user_id} = user} do
      put_user(user)
      assert UserCache.exists?(user_id)
      refute UserCache.exists?(System.unique_integer([:positive]))
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

    test "ignores users without email", %{user: user} do
      user
      |> Map.delete(:email)
      |> put_user()

      assert UserCache.id_to_email_map() == %{}
    end
  end

  describe "refresh_all_roles/1 refresh_resource_roles/3 and get_roles/1" do
    test "refresh_all_roles deletes all entries and load back" do
      %{id: user_id_1} = user1 = build(:user)
      %{id: user_id_2} = user2 = build(:user)
      put_user(user1)
      put_user(user2)

      entries = %{
        user_id_1 => %{
          "domain" => %{
            "role1" => [1, 2, 3],
            "role2" => [4, 5, 6]
          }
        },
        user_id_2 => %{
          "structure" => %{
            "role2" => [3, 4],
            "role3" => [6, 7]
          }
        }
      }

      assert {:ok, [2, 2]} = UserCache.refresh_all_roles(entries)

      assert {:ok,
              %{
                "role1" => [1, 2, 3],
                "role2" => [4, 5, 6]
              }} = UserCache.get_roles(user_id_1, "domain")

      assert {:ok,
              %{
                "role2" => [3, 4],
                "role3" => [6, 7]
              }} = UserCache.get_roles(user_id_2, "structure")

      entries = %{
        user_id_1 => %{"structure" => %{"role4" => [8]}}
      }

      assert {:ok, [2, 1]} = UserCache.refresh_all_roles(entries)

      assert {:ok, nil} = UserCache.get_roles(user_id_1, "domain")
      assert {:ok, nil} = UserCache.get_roles(user_id_2, "structure")
      assert {:ok, %{"role4" => [8]}} = UserCache.get_roles(user_id_1, "structure")

      assert {:ok, [1]} = UserCache.refresh_all_roles(%{})

      assert {:ok, nil} = UserCache.get_roles(user_id_1, "structure")
    end

    test "refresh_all_roles handles empty entries with empty cache" do
      assert {:ok, nil} = UserCache.refresh_all_roles(%{})
    end

    test "refresh_all_roles refreshes multiple resource_types for user" do
      %{id: user_id} = user = build(:user)
      put_user(user)

      entries = %{
        user_id => %{
          "domain" => %{"role1" => [1]},
          "structure" => %{"role2" => [4]}
        }
      }

      assert {:ok, [1, 1]} = UserCache.refresh_all_roles(entries)

      assert {:ok, %{"role1" => [1]}} = UserCache.get_roles(user_id, "domain")
      assert {:ok, %{"role2" => [4]}} = UserCache.get_roles(user_id, "structure")
    end

    test "refresh_resource_roles loads a user roles" do
      %{id: user_id_1} = user1 = build(:user)
      %{id: user_id_2} = user2 = build(:user)
      put_user(user1)
      put_user(user2)

      entries = %{
        "role1" => [1, 2, 3],
        "role2" => [4, 5, 6]
      }

      assert {:ok, [0, 2]} = UserCache.refresh_resource_roles(user_id_1, "domain", entries)

      assert {:ok, ^entries} = UserCache.get_roles(user_id_1, "domain")
      assert {:ok, nil} = UserCache.get_roles(user_id_2, "domain")

      assert {:ok, [0, 2]} = UserCache.refresh_resource_roles(user_id_2, "domain", entries)

      assert {:ok, ^entries} = UserCache.get_roles(user_id_1, "domain")
      assert {:ok, ^entries} = UserCache.get_roles(user_id_2, "domain")

      assert {:ok, [1]} = UserCache.refresh_resource_roles(user_id_1, "domain", %{})

      assert {:ok, nil} = UserCache.get_roles(user_id_1, "domain")
      assert {:ok, ^entries} = UserCache.get_roles(user_id_2, "domain")

      entries2 = %{
        "role3" => [8]
      }

      assert {:ok, [0, 1]} = UserCache.refresh_resource_roles(user_id_2, "structure", entries2)

      assert {:ok, ^entries} = UserCache.get_roles(user_id_2, "domain")
      assert {:ok, ^entries2} = UserCache.get_roles(user_id_2, "structure")

      assert {:ok, [1, 1]} = UserCache.refresh_resource_roles(user_id_2, "domain", entries2)

      assert {:ok, ^entries2} = UserCache.get_roles(user_id_2, "domain")
      assert {:ok, ^entries2} = UserCache.get_roles(user_id_2, "structure")
    end

    test "puts a hash with comma-separated ids of a resource_type as values and reads it back" do
      %{id: user_id} = user = build(:user)
      put_user(user)

      domain_ids_by_role = %{
        "role1_structure" => [10, 20, 30],
        "role2_structure" => [40, 50, 60]
      }

      assert {:ok, [0, 2]} =
               UserCache.refresh_resource_roles(user_id, "structure", domain_ids_by_role)

      assert {:ok, ^domain_ids_by_role} = UserCache.get_roles(user_id, "structure")
    end
  end

  describe "user_groups" do
    test "put_group returns OK" do
      group = build(:group)
      assert {:ok, [_, 2, 1, 1, 1]} = put_user_group(group)
    end

    test "get_group returns a map with name and alias" do
      group = build(:group)
      put_user_group(group)
      {:ok, g} = UserCache.get_group(group.id)
      assert g == Map.take(group, [:name, :alias, :id])
    end

    test "get_group_by_name returns a map with name and alias " do
      group = build(:group)
      put_user_group(group)
      {:ok, g} = UserCache.get_group_by_name(group.name)
      assert g == Map.take(group, [:name, :alias, :id])
    end

    test "get_group_by_name returns a group by alias" do
      group = build(:group)
      put_user_group(group)

      {:ok, g} = UserCache.get_group_by_name(group.alias)

      assert g == Map.take(group, [:name, :alias, :id])
    end

    test "get_group_by_name returns groups from a list of names" do
      group1 = build(:group)
      group2 = build(:group)
      put_user_group(group1)
      put_user_group(group2)

      {:ok, groups} = UserCache.get_group_by_name([group1.name, group2.name])

      assert groups == [
               Map.take(group1, [:name, :alias, :id]),
               Map.take(group2, [:name, :alias, :id])
             ]
    end

    test "get_group_by_name returns groups from a list of group names and user names" do
      group1 = build(:group)
      group2 = build(:group)
      put_user_group(group1)
      put_user_group(group2)

      user1 = build(:user)
      user2 = build(:user)
      put_user(user1)
      put_user(user2)

      {:ok, groups} =
        UserCache.get_group_by_name([group1.name, group2.name, user1.user_name, user2.user_name])

      assert groups == [
               Map.take(group1, [:name, :alias, :id]),
               Map.take(group2, [:name, :alias, :id]),
               nil,
               nil
             ]
    end

    test "delete_group removes group lookups by name and alias" do
      group = build(:group)
      put_user_group(group)

      assert {:ok, %{id: id}} = UserCache.get_group_by_name(group.name)
      assert id == group.id
      assert {:ok, %{id: id}} = UserCache.get_group_by_name(group.alias)
      assert id == group.id

      assert {:ok, _} = UserCache.delete_group(group.id)

      assert {:ok, nil} = UserCache.get_group(group.id)
      assert {:ok, nil} = UserCache.get_group_by_name(group.name)
      assert {:ok, nil} = UserCache.get_group_by_name(group.alias)
    end

    test "delete_group removes group name lookup when alias is nil" do
      group = :group |> build() |> Map.put(:alias, nil)

      put_user_group(group)

      assert {:ok, %{id: id}} = UserCache.get_group_by_name(group.name)
      assert id == group.id

      assert {:ok, _} = UserCache.delete_group(group.id)

      assert {:ok, nil} = UserCache.get_group(group.id)
      assert {:ok, nil} = UserCache.get_group_by_name(group.name)
    end

    test "delete_group does not delete user cache with same id" do
      group = build(:group)

      user = :user |> build() |> Map.put(:id, group.id)

      put_user_group(group)
      put_user(user)

      assert {:ok, %{id: id}} = UserCache.get_group(group.id)
      assert id == group.id
      assert {:ok, %{id: id}} = UserCache.get(user.id)
      assert id == user.id

      assert {:ok, _} = UserCache.delete_group(group.id)

      assert {:ok, nil} = UserCache.get_group(group.id)
      assert {:ok, %{id: id}} = UserCache.get(user.id)
      assert id == user.id
    end
  end

  defp put_user(%{id: id} = user) do
    on_exit(fn -> UserCache.delete(id) end)
    UserCache.put(user)
  end

  defp put_user_group(%{id: id} = group) do
    on_exit(fn -> UserCache.delete_group(id) end)
    UserCache.put_group(group)
  end
end
