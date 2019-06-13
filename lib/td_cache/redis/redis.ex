defmodule TdCache.Redis do
  @moduledoc """
  Redis utility functions
  """

  def hash_to_map(hash) do
    hash
    |> Enum.chunk_every(2)
    |> Map.new(fn [key, value] -> {String.to_atom(key), value} end)
  end

  def hmset(key, %{} = map) when map != %{} do
    entries =
      map
      |> Enum.flat_map(fn {k, v} -> [to_string(k), to_string(v)] end)

    hmset(key, entries)
  end

  def hmset(_, []), do: []

  def hmset(key, [_h | _t] = entries) do
    ["HMSET" | [key | entries]]
  end

  def hmset(_, _), do: []

  def sadd(_, []), do: []

  def sadd(key, [_h | _t] = entries) do
    ["SADD" | [key | entries]]
  end

  def rpush(_, []), do: []

  def rpush(key, [_h | _t] = entries) do
    ["RPUSH" | [key | entries]]
  end

  def read_map(conn, key) do
    case Redix.command(conn, ["HGETALL", key]) do
      {:ok, []} -> {:ok, nil}
      {:ok, hash} -> {:ok, hash_to_map(hash)}
      x -> x
    end
  end

  def read_list(conn, key) do
    Redix.command(conn, ["LRANGE", key, "0", "-1"])
  end
end
