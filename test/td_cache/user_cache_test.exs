defmodule TdCache.UserCacheTest do
  @moduledoc false
  use ExUnit.Case
  alias TdCache.Redix, as: Redis
  alias TdCache.UserCache
  doctest TdCache.UserCache

  setup do
    users =
      0..10
      |> Enum.map(fn _ -> random_user() end)

    on_exit(fn ->
      users
      |> Enum.map(& &1.id)
      |> Enum.each(&UserCache.delete/1)
    end)

    {:ok, users: users}
  end

  test "put returns OK", context do
    [user | _] = context[:users]
    assert {:ok, ["OK", 1]} == UserCache.put(user)
  end

  test "put without email returns OK", context do
    [user | _] = context[:users]
    user = user |> Map.delete(:email)
    assert {:ok, ["OK", 1]} == UserCache.put(user)
  end

  test "get_user returns a map with user_name, full_name and email", context do
    [user | _] = context[:users]
    UserCache.put(user)
    {:ok, u} = UserCache.get(user.id)
    assert u == Map.take(user, [:user_name, :full_name, :email, :id])
  end

  test "get_by_name returns a map with user_name, full_name and email", context do
    [user | _] = context[:users]
    UserCache.put(user)
    {:ok, u} = UserCache.get_by_name(user.full_name)
    assert u == Map.take(user, [:user_name, :full_name, :email, :id])
  end

  test "get_user returns nil if the user is not cached" do
    assert {:ok, nil} == UserCache.get("9876543")
  end

  test "delete_user deletes the user from cache", context do
    [user | _] = context[:users]
    UserCache.put(user)
    UserCache.delete(user.id)
    assert {:ok, 0} = Redis.command(["EXISTS", "user:#{user.id}"])
  end

  defp random_user do
    id = random_id()
    %{id: id, full_name: "user #{id}", email: "user#{id}@foo.bar"}
  end

  defp random_id, do: :rand.uniform(100_000_000)
end
