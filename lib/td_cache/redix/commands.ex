defmodule TdCache.Redix.Commands do
  @moduledoc """
  A module providing functions to create Redix commands.
  """

  @doc """
  Transform multiple commands
  """
  def transform([h | _t] = commands) when is_list(h) do
    commands
    |> Enum.map(&transform/1)
  end

  @doc """
  Convenience function for passing a map to HMSET
  """
  def transform(["HMSET", key, %{} = map]) when map != %{} do
    entries =
      map
      |> Enum.flat_map(fn {k, v} -> [to_string(k), to_string(v)] end)

    ["HMSET", key | entries]
  end

  @doc """
  Convenience function for passing a list to RPUSH
  """
  def transform(["RPUSH", _, []]), do: :ok

  def transform(["RPUSH", key, [_h | _t] = entries]) do
    ["RPUSH", key | entries]
  end

  def transform(x), do: x

  def option(_, nil), do: []
  def option(option, value), do: [option, "#{value}"]
end
