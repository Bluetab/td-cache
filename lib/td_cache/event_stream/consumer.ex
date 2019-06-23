defmodule TdCache.EventStream.Consumer do
  @moduledoc """
  Event Stream Consumer
  """

  use GenServer
  require Logger

  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Stream

  ## Client API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  ## Consumer Behaviour

  @doc """
  Process a list of events

  Returns `:ok` if successful or `{:error, msg}` if it fails
  """
  @callback consume(events :: Enumerable.t()) :: :ok | {:error, String.t()}

  ## Callbacks

  @impl true
  def init(opts) do
    consumer = opts[:consumer]
    quiesce = Keyword.get(opts, :quiesce, 5_000)

    state = %{
      consumer_group: opts[:consumer_group],
      consumer_id: opts[:consumer_id],
      stream: opts[:stream],
      consumer: consumer,
      parent: opts[:parent],
      block: Keyword.get(opts, :block, 1_000),
      count: Keyword.get(opts, :count, 8),
      interval: Keyword.get(opts, :interval, 200)
    }

    Process.send_after(self(), :initialize, quiesce)
    {:ok, state}
  end

  @impl true
  def handle_info(
        :initialize,
        %{
          stream: stream,
          consumer_group: consumer_group,
          consumer_id: consumer_id,
          parent: parent,
          interval: interval
        } = state
      ) do
    {:ok, _} = Stream.create_stream(stream)
    {:ok, _} = Stream.create_consumer_group(stream, consumer_group)

    # Notify parent that initialization has completed (for tests)
    case parent do
      nil -> :ok
      pid -> send(pid, :started)
    end

    Logger.info("Consumer #{consumer_group}:#{consumer_id} for #{stream} initialized")
    Process.send_after(self(), :work, interval)
    {:noreply, state}
  end

  @impl true
  def handle_info(:work, %{interval: interval} = state) do
    do_work(state)
    Process.send_after(self(), :work, interval)
    {:noreply, state}
  end

  ## Private functions

  defp do_work(%{
         stream: stream,
         consumer: consumer,
         consumer_group: consumer_group,
         consumer_id: consumer_id,
         block: block,
         count: count
       }) do
    {:ok, events} =
      Stream.read_group(stream, consumer_group, consumer_id,
        count: count,
        block: block,
        transform: true
      )

    case events do
      [] ->
        :ok

      _ ->
        consumer.consume(events)
        event_ids = events |> Enum.map(& &1.id)
        {:ok, count} = ack(stream, consumer_group, event_ids)
        Logger.info("Consumed #{count} events from #{stream}")
    end
  end

  defp ack(stream, consumer_group, ids) do
    Redis.command(["XACK", stream, consumer_group] ++ ids)
  end
end
