defmodule TdCache.EventStream.Publisher do
  @moduledoc """
  Publishes events to Redis streams.
  """

  alias TdCache.Redix

  require Logger

  def publish(events, stream) when is_list(events) do
    events
    |> Enum.map(&Map.put(&1, :stream, stream))
    |> publish()
  end

  def publish(%{event: _} = map, stream) do
    map
    |> command(stream)
    |> Redix.command()
  end

  def publish(%{event: _, stream: stream} = map) do
    map
    |> command(stream)
    |> Redix.command()
  end

  def publish([]), do: {:ok, []}

  def publish(events) when is_list(events) do
    Logger.debug("Publishing #{Enum.count(events)} events")

    events
    |> Enum.map(&command/1)
    |> Redix.transaction_pipeline()
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
