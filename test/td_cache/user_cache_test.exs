defmodule TdCache.UserCacheTest do
  use ExUnit.Case

  import TdCache.Factory

  alias TdCache.Redix
  alias TdCache.UserCache

  doctest TdCache.UserCache

  setup do
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

  describe "put_roles/2 and get_roles/1" do
    test "puts a hash with comma-separated ids as values and reads it back" do
      %{id: user_id} = user = build(:user)
      put_user(user)

      domain_ids_by_role = %{
        "role1" => [1, 2, 3],
        "role2" => [4, 5, 6]
      }

      assert {:ok, [0, 2]} = UserCache.put_roles(user_id, domain_ids_by_role)
      assert {:ok, ^domain_ids_by_role} = UserCache.get_roles(user_id)
      assert {:ok, ^domain_ids_by_role} = UserCache.get_roles(user_id, "domain")
    end

    test "puts a hash with comma-separated ids of a resource_type as values and reads it back" do
      %{id: user_id} = user = build(:user)
      put_user(user)

      domain_ids_by_role = %{
        "role1_structure" => [10, 20, 30],
        "role2_structure" => [40, 50, 60]
      }

      assert {:ok, [0, 2]} = UserCache.put_roles(user_id, domain_ids_by_role, "structure")
      assert {:ok, ^domain_ids_by_role} = UserCache.get_roles(user_id, "structure")
    end
  end

  defp put_user(%{id: id} = user) do
    on_exit(fn -> UserCache.delete(id) end)
    UserCache.put(user)
  end
end
