defmodule TdCache.EventStream.Consumer do
  @moduledoc """
  Consumes an event stream from Redis.
  """

  use GenServer
  require Logger
  alias TdCache.Redis

  ## Client API

  def start_link(options) do
    {name, options} = Keyword.pop(options, :name, __MODULE__)
    GenServer.start_link(__MODULE__, options, name: name)
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  def read(server, opts \\ []) do
    GenServer.call(server, {:read, opts})
  end

  def ack(server, ids) do
    GenServer.call(server, {:ack, ids})
  end

  def reset(server) do
    GenServer.cast(server, :reset)
  end

  ## Callbacks

  @impl true
  def init(options) do
    stream = Keyword.get(options, :stream)
    group = Keyword.get(options, :group)
    consumer = Keyword.get(options, :consumer)
    {:ok, conn} = Redix.start_link(host: Keyword.get(options, :redis_host, "redis"))
    state = %{stream: stream, group: group, consumer: consumer, conn: conn, status: :starting}
    Process.send_after(self(), :create_group, 0)
    {:ok, state}
  end

  @impl true
  def handle_info(:create_group, %{conn: conn, stream: stream, group: group} = state) do
    create_group(conn, stream, group)
    state = Map.put(state, :status, :started)
    {:noreply, state}
  end

  @impl true
  def handle_call(:status, _from, %{status: status} = state) do
    {:reply, {:ok, status}, state}
  end

  @impl true
  def handle_call(
        {:read, opts},
        _from,
        %{conn: conn, stream: stream, group: group, consumer: consumer} = state
      ) do
    reply = read(conn, stream, group, consumer, opts)
    {:reply, {:ok, reply}, state}
  end

  @impl true
  def handle_call({:ack, ids}, _from, %{conn: conn, stream: stream, group: group} = state) do
    reply = ack(conn, stream, group, ids)
    {:reply, reply, state}
  end

  @impl true
  def handle_cast(:reset, %{conn: conn, stream: stream, group: group} = state) do
    destroy_group(conn, stream, group)
    {:stop, :normal, state}
  end

  ## Private functions

  defp create_group(conn, stream, group) do
    create_stream(conn, stream)
    {:ok, groups} = Redix.command(conn, ["XINFO", "GROUPS", stream])

    unless Enum.any?(groups, &(Enum.at(&1, 1) == group)) do
      command = ["XGROUP", "CREATE", stream, group, "0", "MKSTREAM"]
      {:ok, "OK"} = Redix.command(conn, command)
      Logger.info("Created consumer group #{group} for stream #{stream}")
    end
  end

  defp create_stream(conn, stream) do
    {:ok, type} = Redix.command(conn, ["TYPE", stream])

    case type do
      "stream" ->
        :ok

      "none" ->
        Redix.command(conn, ["XADD", stream, "0-1", "action", "stream_created"])

      _ ->
        raise("Existing key #{stream} is not a stream (type #{type})")
    end
  end

  defp read(conn, stream, group, consumer, opts) do
    count = optional_params("COUNT", Keyword.get(opts, :count))
    block = optional_params("BLOCK", Keyword.get(opts, :block))

    {:ok, streams} =
      Redix.command(
        conn,
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

  defp ack(conn, stream, group, ids) do
    Redix.command(conn, ["XACK", stream, group] ++ ids)
  end

  defp destroy_group(conn, stream, group) do
    Redix.command(conn, ["XGROUP", "DESTROY", stream, group])
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
