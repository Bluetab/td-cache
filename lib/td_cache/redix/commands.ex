defmodule TdCache.Redix.Commands do
  @moduledoc """
  A module providing functions to create Redix commands.
  """

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

  def option(_, nil), do: []
  def option(option, value), do: [option, "#{value}"]
end
