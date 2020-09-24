defmodule TdCache.Redix.Stream do
  @moduledoc """
  Manages access to Redis streams.
  """

  alias TdCache.Redix
  alias TdCache.Redix.Commands

  require Logger

  def create_stream(key) do
    {:ok, type} = Redix.command(["TYPE", key])

    case type do
      "stream" ->
        {:ok, :exists}

      "none" ->
        {:ok, res} =
          Redix.transaction_pipeline([
            ["XADD", key, "0-1", "event", "init"],
            ["XTRIM", key, "MAXLEN", "0"]
          ])

        Logger.info("Created stream #{key}")
        {:ok, res}

      _ ->
        raise("Existing key #{key} of type #{type} is not a stream")
    end
  end

  def create_consumer_group(key, group) do
    {:ok, groups} = Redix.command(["XINFO", "GROUPS", key])

    if Enum.any?(groups, &(Enum.at(&1, 1) == group)) do
      {:ok, :exists}
    else
      command = ["XGROUP", "CREATE", key, group, "0", "MKSTREAM"]
      {:ok, "OK"} = Redix.command(command)
      Logger.info("Created consumer group #{group} for stream #{key}")
      {:ok, :created}
    end
  end

  def destroy_consumer_group(key, group) do
    Redix.command(["XGROUP", "DESTROY", key, group])
    Logger.info("Destroyed consumer group #{group} for stream #{key}")
  end

  def range(redix, stream, start_id, end_id, opts \\ []) do
    count = Commands.option("COUNT", Keyword.get(opts, :count))
    command = ["XRANGE", stream, start_id, end_id] ++ count
    events = read_events(redix, command, opts)

    {:ok, events}
  end

  def read(redix, stream, opts)

  def read(redix, stream, opts) when is_binary(stream) do
    read(redix, [stream], opts)
  end

  def read(redix, streams, opts) when is_list(streams) do
    count = Commands.option("COUNT", Keyword.get(opts, :count))
    block = Commands.option("BLOCK", Keyword.get(opts, :block))
    ids = Keyword.get(opts, :ids, Enum.map(streams, fn _ -> "0-0" end))

    command = ["XREAD"] ++ count ++ block ++ ["STREAMS"] ++ streams ++ ids
    events = read_events(redix, command, opts)

    {:ok, events}
  end

  def read_group(redix, stream, group, consumer, opts \\ []) do
    count = Commands.option("COUNT", Keyword.get(opts, :count))
    block = Commands.option("BLOCK", Keyword.get(opts, :block))

    command =
      ["XREADGROUP", "GROUP", group, consumer] ++ count ++ block ++ ["STREAMS", stream, ">"]

    events = read_events(redix, command, opts)

    {:ok, events}
  end

  def ack(redix, stream, group, ids) do
    Redix.command(redix, ["XACK", stream, group | ids])
  end

  defp read_events(redix, command, opts) do
    case Redix.command(redix, command) do
      {:ok, nil} ->
        []

      {:ok, events_by_stream} ->
        parse_results(events_by_stream, opts)
    end
  end

  def trim(stream, count) do
    Redix.command(["XTRIM", stream, "MAXLEN", count])
  end

  def delete_events(stream, event_ids) do
    Redix.command!(["XDEL", stream | event_ids])
  end
  def delete_if_empty(stream) do
    case Redix.command!(["XLEN", stream]) do
      0 -> Redix.command!(["DEL", stream])
      _ -> :ok
    end
  end

  defp parse_results(events, opts) do
    case Keyword.get(opts, :transform) do
      :range ->
        Enum.map(events, &event_to_map/1)

      true ->
        events
        |> Enum.flat_map(&stream_events/1)
        |> Enum.sort_by(& &1.id)

      _ ->
        events
    end
  end

  defp stream_events([stream, events]) do
    Enum.map(events, &event_to_map(&1, stream))
  end

  defp event_to_map([id, hash]) do
    hash
    |> Redix.hash_to_map()
    |> Map.put(:id, id)
  end

  defp event_to_map([id, hash], stream) do
    [id, hash]
    |> event_to_map()
    |> Map.put(:stream, stream)
  end
end
