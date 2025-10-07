defmodule TdCache.ConfigIntegrationTest do
  use ExUnit.Case

  alias TdCache.Audit
  alias TdCache.EventStream.Publisher
  alias TdCache.Redix

  setup_all do
    on_exit(fn ->
      Redix.del!(["test:stream", "audit:events:test"])
    end)
  end

  setup do
    Redix.del!(["test:stream", "audit:events:test"])

    original_audit_config = Application.get_env(:td_cache, :audit, [])
    original_event_stream_config = Application.get_env(:td_cache, :event_stream, [])

    on_exit(fn ->
      Redix.del!(["test:stream", "audit:events:test"])
      Application.put_env(:td_cache, :audit, original_audit_config)
      Application.put_env(:td_cache, :event_stream, original_event_stream_config)
    end)

    :ok
  end

  describe "environment variable integration" do
    test "reads REDIS_AUDIT_STREAM_MAXLEN from environment" do
      System.put_env("REDIS_AUDIT_STREAM_MAXLEN", "300")

      Application.put_env(:td_cache, :audit,
        service: "td-cache",
        stream: "audit:events:test",
        maxlen: System.get_env("REDIS_AUDIT_STREAM_MAXLEN", "100")
      )

      event = %TdCache.Audit.Event{
        event: "test_event",
        resource_id: 1,
        resource_type: "test",
        user_id: 123,
        payload: %{"data" => "test"}
      }

      assert {:ok, _id} = Audit.publish(event)

      System.delete_env("REDIS_AUDIT_STREAM_MAXLEN")
    end

    test "reads REDIS_STREAM_MAXLEN from environment" do
      System.put_env("REDIS_STREAM_MAXLEN", "500")

      Application.put_env(:td_cache, :event_stream,
        maxlen: System.get_env("REDIS_STREAM_MAXLEN", "100"),
        streams: []
      )

      event = %{event: "test_event", data: "test_data"}

      assert {:ok, _id} = Publisher.publish(event, "test:stream", [])

      System.delete_env("REDIS_STREAM_MAXLEN")
    end

    test "uses default values when environment variables are not set" do
      System.delete_env("REDIS_AUDIT_STREAM_MAXLEN")
      System.delete_env("REDIS_STREAM_MAXLEN")

      Application.put_env(:td_cache, :audit,
        service: "td-cache",
        stream: "audit:events:test",
        maxlen: System.get_env("REDIS_AUDIT_STREAM_MAXLEN", "100")
      )

      Application.put_env(:td_cache, :event_stream,
        maxlen: System.get_env("REDIS_STREAM_MAXLEN", "100"),
        streams: []
      )

      event = %TdCache.Audit.Event{
        event: "test_event",
        resource_id: 1,
        resource_type: "test",
        user_id: 123,
        payload: %{"data" => "test"}
      }

      assert {:ok, _id} = Audit.publish(event)

      publisher_event = %{event: "test_event", data: "test_data"}
      assert {:ok, _id} = Publisher.publish(publisher_event, "test:stream", [])
    end

    test "configuration precedence: opts > config > default" do
      System.put_env("REDIS_STREAM_MAXLEN", "200")

      Application.put_env(:td_cache, :event_stream,
        maxlen: System.get_env("REDIS_STREAM_MAXLEN", "100"),
        streams: []
      )

      event = %{event: "test_event", data: "test_data"}

      assert {:ok, _id} = Publisher.publish(event, "test:stream", maxlen: "999")

      System.delete_env("REDIS_STREAM_MAXLEN")
    end
  end

  describe "stream retention behavior" do
    test "audit stream respects maxlen configuration" do
      Application.put_env(:td_cache, :audit,
        service: "td-cache",
        stream: "audit:events:test",
        maxlen: "2"
      )

      events =
        for i <- 1..5 do
          %TdCache.Audit.Event{
            event: "test_event_#{i}",
            resource_id: i,
            resource_type: "test",
            user_id: 123,
            payload: %{"data" => "test_#{i}"}
          }
        end

      Enum.each(events, &Audit.publish/1)

      Process.sleep(200)

      stream = Audit.stream()
      {:ok, stream_length} = Redix.command(:redix, ["XLEN", stream])

      assert stream_length > 0
      assert stream_length <= 5
    end

    test "event stream respects maxlen configuration" do
      Application.put_env(:td_cache, :event_stream,
        maxlen: "3",
        streams: []
      )

      events =
        for i <- 1..10 do
          %{event: "test_event_#{i}", data: "data_#{i}"}
        end

      Enum.each(events, &Publisher.publish(&1, "test:stream", []))

      Process.sleep(200)

      {:ok, stream_length} = Redix.command(:redix, ["XLEN", "test:stream"])

      assert stream_length > 0
      assert stream_length <= 10
    end
  end

  describe "configuration validation" do
    test "handles invalid maxlen values gracefully" do
      Application.put_env(:td_cache, :event_stream,
        maxlen: "invalid",
        streams: []
      )

      event = %{event: "test_event", data: "test_data"}

      assert {:error, _error} = Publisher.publish(event, "test:stream", [])
    end

    test "handles nil maxlen configuration" do
      Application.put_env(:td_cache, :event_stream,
        maxlen: nil,
        streams: []
      )

      event = %{event: "test_event", data: "test_data"}

      assert {:error, _error} = Publisher.publish(event, "test:stream", [])
    end

    test "uses explicit maxlen option over invalid config" do
      Application.put_env(:td_cache, :event_stream,
        maxlen: "invalid",
        streams: []
      )

      event = %{event: "test_event", data: "test_data"}

      assert {:ok, _id} = Publisher.publish(event, "test:stream", maxlen: "50")
    end
  end
end
