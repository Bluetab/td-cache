defmodule TdCache.Redix.Stream do
  @moduledoc """
  Manages access to Redis streams.
  """
  require Logger
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands

  def create_stream(key) do
    {:ok, type} = Redis.command(["TYPE", key])

    case type do
      "stream" ->
        {:ok, :exists}

      "none" ->
        {:ok, res} =
          Redis.transaction_pipeline([
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
    {:ok, groups} = Redis.command(["XINFO", "GROUPS", key])

    if Enum.any?(groups, &(Enum.at(&1, 1) == group)) do
      {:ok, :exists}
    else
      command = ["XGROUP", "CREATE", key, group, "0", "MKSTREAM"]
      {:ok, "OK"} = Redis.command(command)
      Logger.info("Created consumer group #{group} for stream #{key}")
      {:ok, :created}
    end
  end

  def destroy_consumer_group(key, group) do
    Redis.command(["XGROUP", "DESTROY", key, group])
    Logger.info("Destroyed consumer group #{group} for stream #{key}")
  end

  def read_group(stream, group, consumer, options \\ []) do
    count = Commands.option("COUNT", Keyword.get(options, :count))
    block = Commands.option("BLOCK", Keyword.get(options, :block))

    Redis.command(
      ["XREADGROUP", "GROUP", group, consumer] ++ count ++ block ++ ["STREAMS", stream, ">"]
    )
  end
end
