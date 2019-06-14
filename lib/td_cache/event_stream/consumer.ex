defmodule TdCache.EventStream.Consumer do
  @moduledoc """
  Consumes an event stream from Redis.
  """

  use GenServer
  require Logger
  alias TdCache.Redix, as: Redis

  ## Client API

  def start_link(options) do
    stream = Keyword.get(options, :stream)
    GenServer.start_link(__MODULE__, options, name: String.to_atom(stream))
  end

  def read(stream, opts \\ []) do
    GenServer.call(String.to_atom(stream), {:read, opts})
  end

  def ack(stream, ids) do
    GenServer.call(String.to_atom(stream), {:ack, ids})
  end

  def reset(stream) do
    GenServer.cast(String.to_atom(stream), :reset)
  end

  ## Callbacks

  @impl true
  def init(options) do
    stream = options[:stream]
    group = options[:group]
    consumer = options[:consumer]
    parent = options[:parent]
    state = %{stream: stream, group: group, consumer: consumer, parent: parent}
    Process.send_after(self(), :create_group, 0)
    {:ok, state}
  end

  @impl true
  def handle_info(:create_group, %{stream: stream, group: group} = state) do
    create_group(stream, group)

    case Map.get(state, :parent) do
      nil -> :ok
      pid -> send(pid, :started)
    end

    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:read, opts},
        _from,
        %{stream: stream, group: group, consumer: consumer} = state
      ) do
    reply = read(stream, group, consumer, opts)
    {:reply, {:ok, reply}, state}
  end

  @impl true
  def handle_call({:ack, ids}, _from, %{stream: stream, group: group} = state) do
    reply = ack(stream, group, ids)
    {:reply, reply, state}
  end

  @impl true
  def handle_cast(:reset, %{stream: stream, group: group} = state) do
    destroy_group(stream, group)
    {:stop, :normal, state}
  end

  ## Private functions

  defp create_group(stream, group) do
    create_stream(stream)
    {:ok, groups} = Redis.command(["XINFO", "GROUPS", stream])

    unless Enum.any?(groups, &(Enum.at(&1, 1) == group)) do
      command = ["XGROUP", "CREATE", stream, group, "0", "MKSTREAM"]
      {:ok, "OK"} = Redis.command(command)
      Logger.info("Created consumer group #{group} for stream #{stream}")
    end
  end

  defp create_stream(stream) do
    {:ok, type} = Redis.command(["TYPE", stream])

    case type do
      "stream" ->
        :ok

      "none" ->
        Redis.command(["XADD", stream, "0-1", "action", "stream_created"])

      _ ->
        raise("Existing key #{stream} is not a stream (type #{type})")
    end
  end

  defp read(stream, group, consumer, opts) do
    count = optional_params("COUNT", Keyword.get(opts, :count))
    block = optional_params("BLOCK", Keyword.get(opts, :block))

    {:ok, streams} =
      Redis.command(
        ["XREADGROUP", "GROUP", group, consumer] ++ count ++ block ++ ["STREAMS", stream, ">"]
      )

    case streams do
      nil ->
        []

      _ ->
        streams
        |> Enum.flat_map(&stream_events/1)
        |> Enum.sort_by(& &1.id)
    end
  end

  defp ack(stream, group, ids) do
    Redis.command(["XACK", stream, group] ++ ids)
  end

  defp destroy_group(stream, group) do
    Redis.command(["XGROUP", "DESTROY", stream, group])
    Logger.info("Destroyed group #{group} for stream #{stream}")
  end

  defp optional_params(_, nil), do: []
  defp optional_params(parameter, value), do: [parameter, "#{value}"]

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
