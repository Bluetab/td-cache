defmodule TdCache.EventStream.Publisher do
  @moduledoc """
  Publishes events to Redis streams.
  """

  alias TdCache.Redix

  require Logger

  @default_maxlen "100"

  def publish(_events, _stream \\ nil, _opts \\ [])

  def publish([], _stream, _opts), do: {:ok, []}

  def publish(events, stream, opts) when is_list(events) and is_binary(stream) do
    events
    |> Enum.map(&Map.put(&1, :stream, stream))
    |> publish(nil, opts)
  end

  def publish(%{event: _} = map, stream, opts) when is_binary(stream) do
    map
    |> command(stream, opts)
    |> Redix.command()
  end

  def publish(%{event: _, stream: stream} = map, _stream, opts) when is_list(opts) do
    map
    |> command(stream, opts)
    |> Redix.command()
  end

  def publish(events, _stream, opts) when is_list(events) do
    Logger.debug("Publishing #{Enum.count(events)} events")

    events
    |> Enum.map(&command(&1, nil, opts))
    |> Redix.transaction_pipeline()
  end

  defp command(%{stream: stream} = event, _stream, opts) do
    params =
      event
      |> Map.delete(:stream)
      |> event_params

    ["XADD", stream] ++ retention_args(opts) ++ ["*"] ++ params
  end

  defp command(event, stream, opts) do
    ["XADD", stream] ++ retention_args(opts) ++ ["*"] ++ event_params(event)
  end

  defp event_params(%{} = event) do
    Enum.flat_map(event, fn {k, v} -> [to_string(k), to_string(v)] end)
  end

  defp retention_args(opts) do
    ["MAXLEN", "~", maxlen(opts)]
  end

  defp maxlen(opts) do
    case Keyword.get(opts, :maxlen) do
      nil -> Keyword.get(config(), :maxlen, @default_maxlen)
      maxlen when is_binary(maxlen) -> maxlen
    end
  end

  defp config do
    Application.get_env(:td_cache, :event_stream, [])
  end
end
