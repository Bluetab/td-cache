defmodule TdCache.Audit do
  alias TdCache.Audit.Event
  alias TdCache.EventStream.Publisher

  def publish_all(events) when is_list(events) do
    events
    |> Enum.map(&create_event/1)
    |> Publisher.publish(stream())
  end

  def publish(%Event{} = event) do
    event
    |> create_event()
    |> Publisher.publish(stream())
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

  defp timestamp() do
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

  defp config do
    Application.get_env(:td_cache, :audit, [])
  end
end
