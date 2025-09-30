defmodule TdCache.EventStream.PublisherTest do
  use ExUnit.Case

  alias TdCache.EventStream.Publisher
  alias TdCache.Redix
  alias TdCache.Redix.Stream

  setup_all do
    on_exit(fn ->
      Redix.del!(["test:stream", "test:audit:stream"])
    end)
  end

  setup do
    Redix.del!(["test:stream", "test:audit:stream"])

    on_exit(fn ->
      Redix.del!(["test:stream", "test:audit:stream"])
    end)

    :ok
  end

  describe "publish/3 with maxlen options" do
    test "publishes event with custom maxlen" do
      event = %{event: "test_event", data: "test_data"}
      stream = "test:stream"

      assert {:ok, id} = Publisher.publish(event, stream, maxlen: "50")

      assert {:ok, [published_event]} = Stream.range(:redix, stream, id, id, transform: :range)
      assert published_event.event == "test_event"
      assert published_event.data == "test_data"
    end

    test "publishes event with default maxlen when no options provided" do
      event = %{event: "test_event", data: "test_data"}
      stream = "test:stream"

      assert {:ok, id} = Publisher.publish(event, stream, [])

      assert {:ok, [published_event]} = Stream.range(:redix, stream, id, id, transform: :range)
      assert published_event.event == "test_event"
      assert published_event.data == "test_data"
    end

    test "publishes event with stream in event map" do
      event = %{event: "test_event", stream: "test:stream", data: "test_data"}

      assert {:ok, id} = Publisher.publish(event, nil, maxlen: "25")

      assert {:ok, [published_event]} =
               Stream.range(:redix, "test:stream", id, id, transform: :range)

      assert published_event.event == "test_event"
      assert published_event.data == "test_data"
    end

    test "publishes list of events with maxlen" do
      events = [
        %{event: "event1", data: "data1"},
        %{event: "event2", data: "data2"}
      ]

      assert {:ok, ids} = Publisher.publish(events, "test:stream", maxlen: "100")
      assert length(ids) == 2

      assert {:ok, published_events} =
               Stream.range(:redix, "test:stream", "-", "+", transform: :range)

      assert length(published_events) == 2
    end

    test "returns ok for empty event list" do
      assert {:ok, []} = Publisher.publish([], "test:stream", [])
    end

    test "uses configuration maxlen when no option provided" do
      original_config = Application.get_env(:td_cache, :event_stream, [])
      Application.put_env(:td_cache, :event_stream, maxlen: "200")

      try do
        event = %{event: "test_event", data: "test_data"}
        stream = "test:stream"

        assert {:ok, id} = Publisher.publish(event, stream, [])

        assert {:ok, [published_event]} = Stream.range(:redix, stream, id, id, transform: :range)
        assert published_event.event == "test_event"
      after
        Application.put_env(:td_cache, :event_stream, original_config)
      end
    end
  end

  describe "retention_args/1" do
    test "returns MAXLEN args with custom value" do
      event = %{event: "test_event", data: "test_data"}
      stream = "test:stream"

      assert {:ok, _id} = Publisher.publish(event, stream, maxlen: "75")

      assert {:ok, [_event]} = Stream.range(:redix, stream, "-", "+", transform: :range)
    end
  end

  describe "maxlen/1" do
    test "uses provided maxlen value" do
      event = %{event: "test_event", data: "test_data"}
      stream = "test:stream"

      assert {:ok, _id} = Publisher.publish(event, stream, maxlen: "999")

      assert {:ok, [_event]} = Stream.range(:redix, stream, "-", "+", transform: :range)
    end

    test "falls back to default maxlen when nil" do
      event = %{event: "test_event", data: "test_data"}
      stream = "test:stream"

      assert {:ok, _id} = Publisher.publish(event, stream, [])

      assert {:ok, [_event]} = Stream.range(:redix, stream, "-", "+", transform: :range)
    end
  end

  describe "event_params/1" do
    test "converts event map to flat key-value list" do
      event = %{event: "test_event", data: "test_data", count: 42}
      stream = "test:stream"

      assert {:ok, id} = Publisher.publish(event, stream, [])

      assert {:ok, [published_event]} = Stream.range(:redix, stream, id, id, transform: :range)
      assert published_event.event == "test_event"
      assert published_event.data == "test_data"

      assert published_event.count == "42"
    end
  end
end
