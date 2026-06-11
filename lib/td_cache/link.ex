defmodule TdCache.Link do
  @moduledoc false
  defstruct [
    :id,
    :source,
    :target,
    :origin,
    :updated_at,
    :tags,
    :disabled_at,
    :disabled_reason
  ]
end
