defmodule TdCache.EventStream.Publisher do
  @moduledoc """
  Publishes events to Redis streams.
  """
  alias TdCache.Redix, as: Redis

  def publish(%{event: _} = map, stream) do
    map
    |> command(stream)
    |> Redis.command()
  end

  def publish([]), do: {:ok, []}

  def publish(events) when is_list(events) do
    events
    |> Enum.map(&command/1)
    |> Redis.transaction_pipeline()
  end

  defp command(%{stream: stream} = event) do
    params =
      event
      |> Map.delete(:stream)
      |> event_params

    ["XADD", stream, "*"] ++ params
  end

  defp command(event, stream) do
    ["XADD", stream, "*"] ++ event_params(event)
  end

  defp event_params(%{} = event) do
    Enum.flat_map(event, fn {k, v} -> [to_string(k), to_string(v)] end)
  end
end
