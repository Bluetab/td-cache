defmodule TdCache.EventStream.ConsumerTest do
  use ExUnit.Case
  alias TdCache.EventStream.Consumer
  alias TdCache.EventStream.TestConsumer
  alias TdCache.Redix, as: Redis
  doctest TdCache.EventStream.Consumer

  setup do
    stream = "test:events"

    config = [
      consumer_group: "test_group",
      consumer_id: "test_consumer",
      stream: stream,
      consumer: TestConsumer,
      parent: self()
    ]

    on_exit(fn -> Redis.command!(["DEL", stream]) end)

    {:ok, _pid} = TestConsumer.start_link(parent: self())
    :ok = start_consumer(config)
    {:ok, stream: stream}
  end

  defp start_consumer(config) do
    {:ok, _pid} = Consumer.start_link(config)

    receive do
      :started -> :ok
    after
      2_000 -> :timeout
    end
  end

  describe "EventStream Consumer" do
    test "consumes events on the stream", context do
      stream = context[:stream]
      {:ok, event_id} = Redis.command(["XADD", stream, "*", "foo", "bar"])
      {:consumed, events} = consume_events()
      assert Enum.any?(events, &(&1.id == event_id))
    end
  end

  defp consume_events do
    receive do
      m -> m
    after
      5_000 -> :timeout
    end
  end
end
