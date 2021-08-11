defmodule TdCache.UserCacheTest do
  @moduledoc false
  use ExUnit.Case
  alias TdCache.Redix
  alias TdCache.UserCache
  doctest TdCache.UserCache

  setup do
    users = Enum.map(0..10, fn _ -> random_user() end)

    {:ok, users: users}
  end

  test "put returns OK", context do
    [user | _] = context[:users]
    assert {:ok, [_, "OK", 1, 1, 1]} = put_user(user)
  end

  test "put without full name returns OK", context do
    [user | _] = context[:users]
    user = user |> Map.delete(:full_name)
    assert {:ok, [_, "OK", 1, 1]} = put_user(user)
  end

  test "put without user name returns OK", context do
    [user | _] = context[:users]
    user = user |> Map.delete(:user_name)
    assert {:ok, [_, "OK", 1, 1]} = put_user(user)
  end

  test "put without user and full name returns OK", context do
    [user | _] = context[:users]
    user = user |> Map.delete(:user_name)
    user = user |> Map.delete(:full_name)
    assert {:ok, [_, "OK", 1]} = put_user(user)
  end

  test "put without email returns OK", context do
    [user | _] = context[:users]
    user = user |> Map.delete(:email)
    assert {:ok, [_, "OK", 1, 1, 1]} = put_user(user)
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

  test "delete_user deletes the user from cache", context do
    [user | _] = context[:users]
    put_user(user)
    UserCache.delete(user.id)
    assert not Redix.exists?("user:#{user.id}")
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

  defp random_user do
    id = System.unique_integer([:positive])

    %{
      id: id,
      full_name: "user #{id}",
      user_name: "user_name#{id}",
      email: "user#{id}@foo.bar"
    }
  end

  defp put_user(%{id: id} = user) do
    on_exit(fn -> UserCache.delete(id) end)
    UserCache.put(user)
  end
end
