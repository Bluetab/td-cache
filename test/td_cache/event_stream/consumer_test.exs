defmodule TdCache.EventStream.ConsumerTest do
  use ExUnit.Case
  alias TdCache.EventStream.Consumer
  alias TdCache.Redix, as: Redis
  doctest TdCache.EventStream.Consumer

  setup_all do
    stream = "test:events"
    group = "test_consumer_group"

    options = [
      stream: stream,
      group: group,
      consumer: "default",
      parent: self()
    ]

    {:ok, _} = Redis.command(["DEL", stream])
    :ok = start_consumer(options)
    {:ok, stream: stream, group: group}
  end

  defp start_consumer(options) do
    {:ok, _pid} = Consumer.start_link(options)

    receive do
      :started -> :ok
    after
      1_000 -> :timeout
    end
  end

  describe "EventStream" do
    test "creates a consumer group on startup", context do
      stream = context[:stream]
      group = context[:group]
      {:ok, groups} = Redis.command(["XINFO", "GROUPS", stream])
      assert Enum.any?(groups, fn [_, name, _, _, _, _, _, _] -> name == group end)
    end

    test "consumes and acknowledges an event which is on the stream", context do
      stream = context[:stream]
      {:ok, event_id} = Redis.command(["XADD", stream, "*", "foo", "bar"])

      {:ok, events} = Consumer.read(stream)
      assert Enum.count(events) >= 1
      assert Enum.any?(events, &(Map.get(&1, :foo) == "bar"))
      event_ids = events |> Enum.map(& &1.id)
      assert Enum.member?(event_ids, event_id)

      {:ok, count} = Consumer.ack(stream, event_ids)
      assert count == Enum.count(event_ids)
    end
  end
end
