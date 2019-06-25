defmodule TdCache.Redix do
  @moduledoc """
  A facade for Redix using a pool of connections.
  """

  alias TdCache.Redix.Pool

  def command(command) do
    Pool.command(command)
  end

  def command!(command) do
    Pool.command!(command)
  end

  def transaction_pipeline(commands) do
    Pool.transaction_pipeline(commands)
  end

  def transaction_pipeline!(commands) do
    Pool.transaction_pipeline!(commands)
  end

  def keys!(pattern \\ "*") do
    command!(["KEYS", pattern])
  end

  def del!(pattern \\ "*") do
    case keys!(pattern) do
      [] -> {:ok, 0}
      keys -> command!(["DEL" | keys])
    end
  end

  def hash_to_map(hash) do
    hash_to_map(hash, fn [key, value] -> {String.to_atom(key), value} end)
  end

  def hash_to_map(hash, fun) do
    hash
    |> Enum.chunk_every(2)
    |> Map.new(&fun.(&1))
  end

  def read_map(key) do
    read_map(key, fn [key, value] -> {String.to_atom(key), value} end)
  end

  def read_map(key, transform) when is_function(transform, 1) do
    case command(["HGETALL", key]) do
      {:ok, []} -> {:ok, nil}
      {:ok, hash} -> {:ok, hash_to_map(hash, transform)}
      x -> x
    end
  end

  def read_list(key) do
    command(["LRANGE", key, "0", "-1"])
  end
end
