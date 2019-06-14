defmodule TdCache.EventStream do
  @moduledoc """
  Event Stream supervisor.
  """

  def child_spec(config) do
    consumer = config[:consumer]
    group = config[:group]

    children =
      config[:streams]
      |> Enum.with_index()
      |> Enum.map(fn {stream, i} ->
        Supervisor.child_spec(
          {TdCache.EventStream.Consumer, stream: stream, group: group, consumer: consumer},
          id: {EventStream, i}
        )
      end)

    # Spec for the supervisor that will supervise the EventStream consumers.
    %{
      id: EventStreamSupervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end
end
