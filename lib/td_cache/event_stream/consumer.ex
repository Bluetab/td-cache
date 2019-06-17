defmodule TdCache.EventStream.Consumer do
  @moduledoc """
  Event Stream Consumer
  """

  use GenServer
  require Logger

  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Stream

  ## Consumer Behaviour

  @doc """
  Process a list of events

  Returns `:ok` if successful or `{:error, msg}` if it fails
  """
  @callback consume(events :: Enumerable.t()) :: :ok | {:error, String.t()}

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  ## Callbacks

  @impl true
  def init(options) do
    state = %{
      consumer_group: options[:consumer_group],
      consumer_id: options[:consumer_id],
      stream: options[:stream],
      consumer: options[:consumer],
      parent: options[:parent]
    }

    Process.send_after(self(), :initialize, 0)

    {:ok, state}
  end

  @impl true
  def handle_info(
        :initialize,
        %{stream: stream, consumer_group: consumer_group, parent: parent} = state
      ) do
    {:ok, _} = Stream.create_stream(stream)
    {:ok, _} = Stream.create_consumer_group(stream, consumer_group)

    Process.send_after(self(), :work, 100)

    # Notify parent that initialization has completed (for tests)
    case parent do
      nil -> :ok
      pid -> send(pid, :started)
    end

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
    events = read(stream, consumer_group, consumer_id, count: 8, block: 1_000)

    case events do
      [] ->
        :ok

      _ ->
        consumer.consume(events)
        event_ids = events |> Enum.map(& &1.id)
        {:ok, count} = ack(stream, consumer_group, event_ids)
        Logger.info("Processed #{count} events")
    end
  end

  defp read(stream, consumer_group, consumer_id, options) do
    {:ok, events_by_stream} = Stream.read_group(stream, consumer_group, consumer_id, options)

    case events_by_stream do
      nil ->
        []

      _ ->
        events_by_stream
        |> Enum.flat_map(&stream_events/1)
        |> Enum.sort_by(& &1.id)
    end
  end

  defp ack(stream, consumer_group, ids) do
    Redis.command(["XACK", stream, consumer_group] ++ ids)
  end

  defp stream_events([stream, events]) do
    events |> Enum.map(&event_to_map(stream, &1))
  end

  defp event_to_map(stream, [id, hash]) do
    hash
    |> Redis.hash_to_map()
    |> Map.put(:id, id)
    |> Map.put(:stream, stream)
  end
end
