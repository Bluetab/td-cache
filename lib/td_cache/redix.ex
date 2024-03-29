defmodule TdCache.Redix do
  @moduledoc """
  A facade for Redix using a pool of connections, with some common utility
  functions.
  """

  alias Redix
  alias TdCache.Redix.Commands
  alias TdCache.Redix.Pool

  def command(pid, command) do
    Redix.command(pid, Commands.transform(command))
  end

  def command(command) do
    command
    |> Commands.transform()
    |> Pool.command()
  end

  def command!(command) do
    command
    |> Commands.transform()
    |> Pool.command!()
  end

  def transaction_pipeline(commands) do
    commands
    |> Commands.transform()
    |> Pool.transaction_pipeline()
  end

  def transaction_pipeline!(commands) do
    commands
    |> Commands.transform()
    |> Pool.transaction_pipeline!()
  end

  def exists?(key) do
    command!(["EXISTS", key]) == 1
  end

  def keys!(pattern \\ "*") do
    command!(["KEYS", pattern])
  end

  def del!(pattern \\ "*")

  def del!(patterns) when is_list(patterns) do
    case Enum.flat_map(patterns, &keys!/1) do
      [] -> {:ok, 0}
      keys -> command!(["DEL" | keys])
    end
  end

  def del!(pattern) do
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

  @spec read_map!(any) :: nil | map | {:error, term}
  def read_map!(key) do
    case read_map(key) do
      {:ok, map} -> map
      error -> error
    end
  end

  @spec read_map(any) :: {:error, term()} | {:ok, nil | map}
  def read_map(key) do
    read_map(key, fn [key, value] -> {String.to_atom(key), value} end)
  end

  @spec read_map(any, (any -> any)) :: {:error, term()} | {:ok, nil | map}
  def read_map(key, transform) when is_function(transform, 1) do
    case command(["HGETALL", key]) do
      {:ok, []} -> {:ok, nil}
      {:ok, hash} -> {:ok, hash_to_map(hash, transform)}
      {:error, error} -> {:error, error}
    end
  end

  def read_list(key) do
    command(["LRANGE", key, "0", "-1"])
  end

  def acquire_lock?(key) do
    command!(["SET", key, node(), "NX"]) == "OK"
  end

  def acquire_lock?(key, expiry_seconds) do
    command!(["SET", key, node(), "NX", "EX", expiry_seconds]) == "OK"
  end

  @spec to_integer_list!(nil | binary | list, binary) :: [integer]
  def to_integer_list!(value, sep \\ ",")

  def to_integer_list!(nil, _), do: []

  def to_integer_list!("", _), do: []

  def to_integer_list!(value, sep) when is_binary(value) do
    value
    |> String.split(sep)
    |> to_integer_list!()
  end

  def to_integer_list!([], _), do: []

  def to_integer_list!([v | _] = values, _) when is_binary(v) do
    Enum.map(values, &String.to_integer/1)
  end
end
