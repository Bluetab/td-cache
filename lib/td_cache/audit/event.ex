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
    payload: %{},
    ts: DateTime.utc_now()
  ]

  @type t :: %__MODULE__{
          event: String.t(),
          payload: Map.t(),
          resource_id: integer(),
          resource_type: String.t(),
          ts: DateTime.t(),
          user_id: integer()
        }
end
