defmodule TdCache.Redix.Commands do
  @moduledoc """
  A module providing functions to create Redix commands.
  """

  @doc """
  Convenience function for preparing Redis commands passing non-string arguments.
  """
  def transform(commands)

  # Convenience function transforming multiple commands.
  def transform([h | _t] = commands) when is_list(h) do
    Enum.map(commands, &transform/1)
  end

  # Convenience function for passing a map to HMSET. HMSET of an empty map deletes the key.
  def transform(["HMSET", key, %{} = map]) when map == %{} do
    ["DEL", key]
  end

  # Convenience function for passing a map to HMSET
  def transform(["HMSET", key, %{} = map]) when map != %{} do
    entries = Enum.flat_map(map, fn {k, v} -> [to_string(k), to_string(v)] end)
    ["HMSET", key | entries]
  end

  def transform(["RPUSH", key, [_h | _t] = entries]) do
    ["RPUSH", key | entries]
  end

  def transform(x), do: x

  def option(_, nil), do: []
  def option(option, value), do: [option, "#{value}"]
end
