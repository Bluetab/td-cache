defmodule TdCache.UserCache do
  @moduledoc """
  Shared cache for users.
  """
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands

  ## Client API

  def get(id) do
    user = read_user(id)
    {:ok, user}
  end

  def get_by_name(name) do
    user = read_by_name(name)
    {:ok, user}
  end

  def get_by_name!(name) do
    case get_by_name(name) do
      {:ok, user} -> user
      error -> error
    end
  end

  def put(user) do
    put_user(user)
  end

  def delete(id) do
    delete_user(id)
  end

  ## Private functions

  @props [:user_name, :full_name, :email]
  @name_to_id_key "users:name_to_id"

  defp read_user(id) when is_binary(id) do
    id
    |> String.to_integer()
    |> read_user
  end

  defp read_user(id) do
    case Redis.read_map("user:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, user} ->
        user
        |> Map.put(:id, id)
    end
  end

  defp read_by_name(full_name) do
    case Redis.command!(["HGET", @name_to_id_key, full_name]) do
      nil -> nil
      id -> read_user(id)
    end
  end

  defp put_user(%{id: id, full_name: full_name} = user) do
    commands = [
      Commands.hmset("user:#{id}", Map.take(user, @props)),
      ["HSET", @name_to_id_key, full_name, id]
    ]

    Redis.transaction_pipeline(commands)
  end

  defp put_user(%{id: id} = user) do
    "user:#{id}"
    |> Commands.hmset(Map.take(user, @props))
    |> Redis.command()
  end

  defp delete_user(id) do
    key = "user:#{id}"

    commands =
      case Redis.command!(["HGET", key, :full_name]) do
        nil ->
          [["DEL", "user:#{id}"]]

        name ->
          [["DEL", "user:#{id}"], ["HDEL", @name_to_id_key, name]]
      end

    Redis.transaction_pipeline(commands)
  end
end
