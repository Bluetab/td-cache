defmodule TdCache.UserCache do
  @moduledoc """
  Shared cache for users.
  """

  alias TdCache.Redix

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
    case Redix.read_map("user:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, user} ->
        user
        |> Map.put(:id, id)
    end
  end

  defp read_by_name(full_name) do
    case Redix.command!(["HGET", @name_to_id_key, full_name]) do
      nil -> nil
      id -> read_user(id)
    end
  end

  defp put_user(%{id: id, full_name: full_name} = user) do
    Redix.transaction_pipeline([
      ["HMSET", "user:#{id}", Map.take(user, @props)],
      ["HSET", @name_to_id_key, full_name, id]
    ])
  end

  defp put_user(%{id: id} = user) do
    Redix.command(["HMSET", "user:#{id}", Map.take(user, @props)])
  end

  defp delete_user(id) do
    case Redix.command!(["HGET", "user:#{id}", :full_name]) do
      nil ->
        Redix.command(["DEL", "user:#{id}"])

      name ->
        Redix.transaction_pipeline([
          ["DEL", "user:#{id}"],
          ["HDEL", @name_to_id_key, name]
        ])
    end
  end
end
