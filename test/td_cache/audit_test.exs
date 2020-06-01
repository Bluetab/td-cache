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

      assert {:ok, [e]} =
               Stream.range(:redix, Audit.stream(), id, id, transform: :range)

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
  end
end
