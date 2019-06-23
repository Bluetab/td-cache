defmodule TdCache.EventStream do
  @moduledoc """
  Event Stream supervisor.
  """

  def child_spec(config) do
    consumer_id = config[:consumer_id]
    consumer_group = config[:consumer_group]

    children =
      config[:streams]
      |> Enum.with_index()
      |> Enum.flat_map(fn {stream_config, i} ->
        stream_workers(stream_config, consumer_group, consumer_id, i)
      end)

    # Spec for the supervisor that will supervise the EventStream consumers.
    %{
      id: EventStreamSupervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end

  defp stream_workers(stream_config, consumer_group, consumer_id, i) do
    stream = stream_config[:key]
    consumer = stream_config[:consumer]
    block = Keyword.get(stream_config, :block, 1_000)
    count = Keyword.get(stream_config, :count, 8)
    quiesce = Keyword.get(stream_config, :quiesce, 5_000)
    interval = Keyword.get(stream_config, :interval, 200)

    [
      Supervisor.child_spec(
        {TdCache.EventStream.Consumer,
         stream: stream,
         consumer_group: consumer_group,
         consumer_id: consumer_id,
         consumer: consumer,
         block: block,
         count: count,
         quiesce: quiesce,
         interval: interval},
        id: {EventStream.Consumer, i}
      )
    ]
  end
end
