defmodule TdCache.TestOperators do
  @moduledoc """
  Equality operators for tests
  """

  def a <~> b, do: approximately_equal(a, b)

  defp approximately_equal([h | t], [h2 | t2]) do
    approximately_equal(h, h2) && approximately_equal(t, t2)
  end

  defp approximately_equal(a, b), do: string_values(a) == string_values(b)

  defp string_values(%{} = map) do
    map
    |> Enum.map(fn
      {k, v} when is_list(v) -> {k, Enum.map(v, &to_string/1)}
      {k, v} -> {k, to_string(v)}
    end)
    |> Map.new()
  end
end
