defmodule TdCache.Audit do
  @moduledoc """
  Support for publishing audit events
  """

  alias TdCache.Audit.Event
  alias TdCache.EventStream.Publisher

  @default_maxlen "100"

  def publish_all(events) when is_list(events) do
    maxlen = maxlen()

    events
    |> Enum.map(&create_event/1)
    |> Publisher.publish(stream(), maxlen: maxlen)
  end

  def publish(%Event{} = event) do
    maxlen = maxlen()

    event
    |> create_event()
    |> Publisher.publish(stream(), maxlen: maxlen)
  end

  def publish(fields) do
    Event
    |> struct(fields)
    |> publish()
  end

  defp create_event(%Event{payload: payload} = event) do
    event
    |> Map.from_struct()
    |> Map.put(:payload, Jason.encode!(payload))
    |> Map.put(:ts, timestamp())
    |> Map.put_new(:service, service())
  end

  defp create_event(fields) do
    Event
    |> struct(fields)
    |> create_event()
  end

  defp timestamp do
    DateTime.utc_now()
    |> DateTime.to_iso8601()
  end

  def stream do
    config()
    |> Keyword.get(:stream, "audit:events")
  end

  defp service do
    config()
    |> Keyword.get(:service, "missing")
  end

  defp maxlen do
    Keyword.get(config(), :maxlen, @default_maxlen)
  end

  defp config do
    Application.get_env(:td_cache, :audit, [])
  end
end
