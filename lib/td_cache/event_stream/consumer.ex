defmodule TdCache.EventStream.Consumer do
  @moduledoc """
  Event Stream Consumer
  """

  use GenServer

  alias TdCache.Redix.Stream

  require Logger

  ## Consumer Behaviour

  @doc """
  Process a list of events

  Returns `:ok` if successful or `{:error, msg}` if it fails
  """
  @callback consume(events :: Enumerable.t()) :: :ok | {:error, String.t()}

  ## Client API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  ## Callbacks

  @impl true
  def init(opts) do
    state = %{
      consumer_group: opts[:consumer_group],
      consumer_id: opts[:consumer_id],
      stream: opts[:stream],
      consumer: opts[:consumer],
      parent: opts[:parent],
      block: Keyword.get(opts, :block, 1_000),
      count: Keyword.get(opts, :count, 8),
      interval: Keyword.get(opts, :interval, 200),
      redix: start_redix(opts)
    }

    quiesce = Keyword.get(opts, :quiesce, 5_000)
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

  defp start_redix(opts) do
    # Starts a dedicated Redis connection for the consumer
    redis_host = Keyword.get(opts, :redis_host, "redis")
    port = Keyword.get(opts, :port, 6379)
    {:ok, redix} = Redix.start_link(host: redis_host, port: port)
    redix
  end

  defp do_work(%{
         redix: redix,
         stream: stream,
         consumer: consumer,
         consumer_group: consumer_group,
         consumer_id: consumer_id,
         block: block,
         count: count
       }) do
    {:ok, events} =
      Stream.read_group(redix, stream, consumer_group, consumer_id,
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
        {:ok, count} = ack(redix, stream, consumer_group, event_ids)
        Logger.info("Consumed #{count} events from #{stream}")
    end
  end

  defp ack(redix, stream, consumer_group, ids) do
    Redix.command(redix, ["XACK", stream, consumer_group] ++ ids)
  end
end
