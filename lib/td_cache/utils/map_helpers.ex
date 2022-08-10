defmodule TdCache.Utils.MapHelpers do
  @moduledoc false

  def parse_fields(nil, _), do: nil

  def parse_fields(map, props) do
    Enum.reduce(props, map, fn {key, type}, map ->
      Map.put(map, key, parse_string(type, Map.get(map, key)))
    end)
  end

  def parse_string(_, ""), do: nil
  def parse_string(_, nil), do: nil
  def parse_string(:integer, value) when is_binary(value), do: String.to_integer(value)
  def parse_string(:decimal, value) when is_binary(value), do: Decimal.new(value)
  def parse_string(:float, value) when is_binary(value), do: String.to_float(value)
  def parse_string(:string, value) when is_atom(value), do: Atom.to_string(value)
  # def parse_string(:datetime, value) when is_binary(value), do: DateTime.from_iso8601(value)
  def parse_string(_, value), do: value
end
