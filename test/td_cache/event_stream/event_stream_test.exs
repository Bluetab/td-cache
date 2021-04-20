defmodule TdCache.EventStream.Test do
  use ExUnit.Case
  alias TdCache.EventStream
  doctest TdCache.EventStream

  setup do
    config = [
      consumer_group: "test_group",
      consumer_id: "test_consumer",
      streams: [
        [key: "foo:events", consumer: FooConsumer],
        [key: "bar:events", consumer: BarConsumer],
        [key: "baz:events", consumer: BarConsumer, group: "baz"]
      ]
    ]

    {:ok, config: config}
  end

  describe "EventStream Consumer" do
    test "child_spec/1 uses defaults for redis_host and port", context do
      config = context[:config]
      %{start: {Supervisor, :start_link, [child_specs | _]}} = EventStream.child_spec(config)

      child_specs
      |> Enum.each(fn %{start: {TdCache.EventStream.Consumer, :start_link, [opts]}} ->
        assert Keyword.get(opts, :redis_host) == "redis"
        assert Keyword.get(opts, :port) == 6379
      end)
    end

    test "child_spec/1 uses specified values for redis_host and port", context do
      config =
        context[:config]
        |> Keyword.put(:redis_host, "foo")
        |> Keyword.put(:port, 1234)

      %{start: {Supervisor, :start_link, [child_specs | _]}} = EventStream.child_spec(config)

      child_specs
      |> Enum.each(fn %{start: {TdCache.EventStream.Consumer, :start_link, [opts]}} ->
        assert Keyword.get(opts, :redis_host) == "foo"
        assert Keyword.get(opts, :port) == 1234
      end)
    end

    test "child_spec/1 uses default config given consumer group", context do
      config = context[:config]
      %{start: {Supervisor, :start_link, [[foo_spec, _, _], _]}} = EventStream.child_spec(config)
      assert %{start: {_, _, [opts]}} = foo_spec
      assert opts[:consumer_group] == "test_group"
    end

    test "child_spec/1 uses consumer-specific consumer group", context do
      config = context[:config]
      %{start: {Supervisor, :start_link, [[_, _, baz_spec], _]}} = EventStream.child_spec(config)
      assert %{start: {_, _, [opts]}} = baz_spec
      assert opts[:consumer_group] == "baz"
    end
  end
end
