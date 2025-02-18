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

  def atomize_keys(nil), do: nil

  # Structs don't do enumerable and anyway the keys are already
  # atoms
  def atomize_keys(%{__struct__: _} = struct) do
    struct
  end

  def atomize_keys(%{} = map) do
    map
    |> Enum.into(%{}, fn {k, v} -> {to_atom_key(k), atomize_keys(v)} end)
  end

  # Walk the list and atomize the keys of
  # of any map members
  def atomize_keys([head | rest]) do
    [atomize_keys(head) | atomize_keys(rest)]
  end

  def atomize_keys(not_a_map) do
    not_a_map
  end

  @doc """
  Convert map atom keys to strings
  """
  def stringify_keys(nil), do: nil

  def stringify_keys(%{} = map) do
    Enum.into(map, %{}, fn {k, v} -> {to_string_key(k), stringify_keys(v)} end)
  end

  # Walk the list and stringify the keys of
  # of any map members
  def stringify_keys([head | rest]) do
    [stringify_keys(head) | stringify_keys(rest)]
  end

  def stringify_keys(not_a_map) do
    not_a_map
  end

  defp to_atom_key(key) when is_binary(key) do
    String.to_atom(key)
  end

  defp to_atom_key(key), do: key

  defp to_string_key(key) when is_atom(key) do
    Atom.to_string(key)
  end

  defp to_string_key(key), do: Atom.to_string(key)
end
