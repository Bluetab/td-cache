defmodule TdCache.CacheCleaner do
  @moduledoc """
  Eliminates deprecated or unused cache entries in Redis
  """

  alias TdCache.Redix

  ## Client API

  def clean(patterns) do
    clean_deprecated_entries(patterns)
  end

  ## Private functions

  defp clean_deprecated_entries(patterns) do
    Enum.each(patterns, &clean_entries/1)
  end

  defp clean_entries(pattern) do
    Redix.del!(pattern)
  end
end
