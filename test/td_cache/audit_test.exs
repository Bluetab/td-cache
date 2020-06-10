defmodule TdCache.AuditTest do
  use ExUnit.Case

  alias TdCache.Audit
  alias TdCache.Audit.Event
  alias TdCache.Redix
  alias TdCache.Redix.Stream

  setup_all do
    on_exit(fn ->
      Audit.stream()
      |> Redix.del!()
    end)
  end

  setup do
    [
      event: %Event{
        event: "test_event",
        resource_id: 44,
        resource_type: "resource_type",
        user_id: 123,
        payload: %{"foo" => "bar"}
      }
    ]
  end

  describe "publish/1" do
    test "publishes an event to the audit stream", %{event: event} do
      assert {:ok, id} = Audit.publish(event)

      assert {:ok, [e]} = Stream.range(:redix, Audit.stream(), id, id, transform: :range)

      assert %{
               event: "test_event",
               id: ^id,
               payload: "{\"foo\":\"bar\"}",
               resource_id: "44",
               resource_type: "resource_type",
               user_id: "123",
               service: "td-cache"
             } = e
    end

    test "sets the timestamp when publishing an event", %{event: event} do
      assert ts_before = DateTime.utc_now()
      assert {:ok, id} = Audit.publish(event)
      assert ts_after = DateTime.utc_now()

      assert {:ok, [e]} = Stream.range(:redix, Audit.stream(), id, id, transform: :range)

      assert %{ts: ts} = e

      assert {:ok, ts, 0} = DateTime.from_iso8601(ts)
      assert DateTime.compare(ts, ts_before) == :gt
      assert DateTime.compare(ts, ts_after) == :lt
    end
  end
end
