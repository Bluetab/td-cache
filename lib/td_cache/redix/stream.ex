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

  def read(stream, options \\ [])

  def read(stream, options) when is_binary(stream) do
    read([stream], options)
  end

  def read(streams, options) when is_list(streams) do
    count = Commands.option("COUNT", Keyword.get(options, :count))
    block = Commands.option("BLOCK", Keyword.get(options, :block))
    ids = Keyword.get(options, :ids, Enum.map(streams, fn _ -> "0-0" end))

    command = ["XREAD"] ++ count ++ block ++ ["STREAMS"] ++ streams ++ ids
    events = read_events(command, options)

    {:ok, events}
  end

  def read_group(stream, group, consumer, options \\ []) do
    count = Commands.option("COUNT", Keyword.get(options, :count))
    block = Commands.option("BLOCK", Keyword.get(options, :block))

    command =
      ["XREADGROUP", "GROUP", group, consumer] ++ count ++ block ++ ["STREAMS", stream, ">"]

    events = read_events(command, options)

    {:ok, events}
  end

  defp read_events(command, options) do
    case Redix.command(command) do
      {:ok, nil} ->
        []

      {:ok, events_by_stream} ->
        parse_results(events_by_stream, options)
    end
  end

  def trim(stream, count) do
    Redix.command(["XTRIM", stream, "MAXLEN", count])
  end

  defp parse_results(events_by_stream, options) do
    case Keyword.get(options, :transform) do
      true ->
        events_by_stream
        |> Enum.flat_map(&stream_events/1)
        |> Enum.sort_by(& &1.id)

      _ ->
        events_by_stream
    end
  end

  defp stream_events([stream, events]) do
    events |> Enum.map(&event_to_map(stream, &1))
  end

  defp event_to_map(stream, [id, hash]) do
    hash
    |> Redix.hash_to_map()
    |> Map.put(:id, id)
    |> Map.put(:stream, stream)
  end
end
