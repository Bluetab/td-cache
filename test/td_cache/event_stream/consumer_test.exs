defmodule TdCache.EventStream.ConsumerTest do
  use ExUnit.Case
  alias TdCache.EventStream.Consumer
  doctest TdCache.EventStream.Consumer

  setup_all do
    stream = "test:events"
    group = "test_consumer_group"

    options = [
      name: :test_event_stream,
      stream: stream,
      group: group,
      consumer: "default"
    ]

    {:ok, conn} = Redix.start_link(host: "redis")
    {:ok, _} = Redix.command(conn, ["DEL", stream])
    start_consumer(options)
    {:ok, stream: stream, group: group, conn: conn}
  end

  defp start_consumer(options) do
    {:ok, _pid} = Consumer.start_link(options)
    wait_for_startup()
  end

  defp wait_for_startup do
    case Consumer.status(:test_event_stream) do
      {:ok, :starting} -> wait_for_startup()
      {:ok, _} -> :ok
    end
  end

  describe "EventStream" do
    test "creates a consumer group on startup", context do
      stream = context[:stream]
      group = context[:group]
      conn = context[:conn]
      {:ok, groups} = Redix.command(conn, ["XINFO", "GROUPS", stream])
      assert Enum.any?(groups, fn [_, name, _, _, _, _, _, _] -> name == group end)
    end

    test "returns it's status (useful for tests)" do
      {:ok, status} = Consumer.status(:test_event_stream)
      assert status == :started
    end

    test "consumes and acknowledges an event which is on the stream", context do
      stream = context[:stream]
      conn = context[:conn]
      {:ok, event_id} = Redix.command(conn, ["XADD", stream, "*", "foo", "bar"])

      {:ok, events} = Consumer.read(:test_event_stream)
      assert Enum.count(events) >= 1
      assert Enum.any?(events, &(Map.get(&1, :foo) == "bar"))
      event_ids = events |> Enum.map(& &1.id)
      assert Enum.member?(event_ids, event_id)

      {:ok, count} = Consumer.ack(:test_event_stream, event_ids)
      assert count == Enum.count(event_ids)
    end
  end
end
