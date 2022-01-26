defmodule TdCache.Audit.Event do
  @moduledoc "A struct representing an Audit Event"

  @enforce_keys [
    :event,
    :resource_id,
    :resource_type,
    :user_id
  ]
  defstruct [
    :event,
    :resource_id,
    :resource_type,
    :user_id,
    payload: %{}
  ]

  @type t :: %__MODULE__{
          event: String.t(),
          payload: map(),
          resource_id: integer(),
          resource_type: String.t(),
          user_id: integer()
        }
end
