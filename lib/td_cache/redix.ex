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

  def hash_to_map(hash) do
    hash
    |> Enum.chunk_every(2)
    |> Map.new(fn [key, value] -> {String.to_atom(key), value} end)
  end

  def read_map(key) do
    case command(["HGETALL", key]) do
      {:ok, []} -> {:ok, nil}
      {:ok, hash} -> {:ok, hash_to_map(hash)}
      x -> x
    end
  end

  def read_list(key) do
    command(["LRANGE", key, "0", "-1"])
  end
end
