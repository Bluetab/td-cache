defmodule TdCache.EventStream do
  @moduledoc """
  Event Stream supervisor.
  """

  def child_spec(config) do
    consumer_config = Keyword.take(config, [:redis_host, :port, :password, :consumer_id, :consumer_group])

    children =
      config[:streams]
      |> Enum.with_index()
      |> Enum.flat_map(fn {stream_config, i} ->
        stream_workers(consumer_config, stream_config, i)
      end)

    # Spec for the supervisor that will supervise the EventStream consumers.
    %{
      id: EventStreamSupervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end

  defp stream_workers(consumer_config, stream_config, i) do
    [
      Supervisor.child_spec(
        {TdCache.EventStream.Consumer,
         redis_host: Keyword.get(consumer_config, :redis_host, "redis"),
         port: Keyword.get(consumer_config, :port, 6379),
         password: Keyword.get(consumer_config, :password),
         stream: stream_config[:key],
         consumer_group: consumer_config[:consumer_group],
         consumer_id: consumer_config[:consumer_id],
         consumer: stream_config[:consumer],
         block: Keyword.get(stream_config, :block, 1_000),
         count: Keyword.get(stream_config, :count, 8),
         quiesce: Keyword.get(stream_config, :quiesce, 5_000),
         interval: Keyword.get(stream_config, :interval, 200)},
        id: {EventStream.Consumer, i}
      )
    ]
  end
end
