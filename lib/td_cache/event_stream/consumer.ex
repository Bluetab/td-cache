defmodule TdCache.EventStream.Consumer do
  @moduledoc """
  Event Stream Consumer
  """

  use GenServer
  require Logger

  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Stream

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  ## Consumer Behaviour

  @doc """
  Process a list of events

  Returns `:ok` if successful or `{:error, msg}` if it fails
  """
  @callback consume(events :: Enumerable.t()) :: :ok | {:error, String.t()}

  ## Callbacks

  @impl true
  def init(options) do
    consumer = options[:consumer]

    state = %{
      consumer_group: options[:consumer_group],
      consumer_id: options[:consumer_id],
      stream: options[:stream],
      consumer: consumer,
      parent: options[:parent]
    }

    Process.send_after(self(), :initialize, 0)
    {:ok, state}
  end

  @impl true
  def handle_info(
        :initialize,
        %{
          stream: stream,
          consumer_group: consumer_group,
          consumer_id: consumer_id,
          parent: parent
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
    Process.send_after(self(), :work, 0)
    {:noreply, state}
  end

  @impl true
  def handle_info(
        :work,
        %{
          stream: stream,
          consumer: consumer,
          consumer_group: consumer_group,
          consumer_id: consumer_id
        } = state
      ) do
    do_work(consumer, stream, consumer_group, consumer_id)
    Process.send_after(self(), :work, 0)
    {:noreply, state}
  end

  ## Private functions

  defp do_work(consumer, stream, consumer_group, consumer_id) do
    {:ok, events} =
      Stream.read_group(stream, consumer_group, consumer_id,
        count: 16,
        block: 1_000,
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
