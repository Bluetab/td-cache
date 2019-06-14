defmodule TdCache.LinkCache do
  @moduledoc """
  Shared cache for links between entities.
  """
  use GenServer

  alias TdCache.Redix, as: Redis

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @doc """
  Creates cache entries relating to a given link.
  """
  def put(link) do
    GenServer.call(__MODULE__, {:put, link})
  end

  @doc """
  Reads a cache entries relating to a given link id.
  """
  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  @doc """
  Deletes cache entries relating to a given link id.
  """
  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  ## Callbacks

  @impl true
  def init(_args) do
    state = %{}
    {:ok, state}
  end

  @impl true
  def handle_call({:put, link}, _from, state) do
    reply = put_link(link)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    reply = get_link(id)
    {:reply, {:ok, reply}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_link(id)
    {:reply, reply, state}
  end

  ## Private functions

  defp get_link(id) do
    {:ok, tags} = Redis.read_list("link:#{id}:tags")
    {:ok, link} = Redis.read_map("link:#{id}")

    case link do
      nil -> nil
      l -> Map.put(l, :tags, tags)
    end
  end

  defp put_link(
         %{
           id: id,
           source_type: source_type,
           source_id: source_id,
           target_type: target_type,
           target_id: target_id
         } = link
       ) do
    commands = put_link_commands(link)

    {:ok, results} = Redis.transaction_pipeline(commands)
    source_add_count = Enum.at(results, 2)
    target_add_count = Enum.at(results, 3)

    unless source_add_count == 0 do
      {:ok, _source_event_id} =
        Redis.command(
          event_command(
            "#{source_type}:events",
            "add_link",
            "link:#{id}",
            "#{source_type}:#{source_id}",
            "#{target_type}:#{target_id}"
          )
        )
    end

    unless target_add_count == 0 do
      {:ok, _target_event_id} =
        Redis.command(
          event_command(
            "#{target_type}:events",
            "add_link",
            "link:#{id}",
            "#{source_type}:#{source_id}",
            "#{target_type}:#{target_id}"
          )
        )
    end

    {:ok, results}
  end

  defp put_link_commands(
         %{
           id: id,
           source_type: source_type,
           source_id: source_id,
           target_type: target_type,
           target_id: target_id,
           updated_at: ts
         } = link
       ) do
    [
      ["DEL", "link:#{id}", "link:#{id}:tags"],
      [
        "HMSET",
        "link:#{id}",
        "source",
        "#{source_type}:#{source_id}",
        "target",
        "#{target_type}:#{target_id}",
        "ts",
        to_string(ts)
      ],
      ["SADD", "#{source_type}:#{source_id}:links", "link:#{id}"],
      ["SADD", "#{target_type}:#{target_id}:links", "link:#{id}"]
    ] ++ put_link_tags_commands(link)
  end

  defp put_link_tags_commands(%{id: id, tags: tags}) do
    [
      ["SADD", "link:#{id}:tags"] ++ tags
    ]
  end

  defp put_link_tags_commands(_), do: []

  def delete_link(id) do
    {:ok, keys} = Redis.command(["HMGET", "link:#{id}", "source", "target"])
    delete_link(id, keys)
  end

  defp delete_link(id, [nil, nil]) do
    Redis.command(["DEL", "link:#{id}", "link:#{id}:tags"])
  end

  defp delete_link(id, [source, target]) do
    [source_type, _source_id] = String.split(source, ":")
    [target_type, _target_id] = String.split(target, ":")

    commands = [
      ["SREM", "#{source}:links", "link:#{id}"],
      ["SREM", "#{target}:links", "link:#{id}"],
      ["DEL", "link:#{id}", "link:#{id}:tags"]
    ]

    {:ok, results} = Redis.transaction_pipeline(commands)
    [source_del_count, target_del_count, _] = results

    unless source_del_count == 0 do
      {:ok, _source_event_id} =
        Redis.command(
          event_command("#{source_type}:events", "remove_link", "link:#{id}", source, target)
        )
    end

    unless target_del_count == 0 do
      {:ok, _target_event_id} =
        Redis.command(
          event_command("#{target_type}:events", "remove_link", "link:#{id}", source, target)
        )
    end

    {:ok, results}
  end

  defp event_command(stream, action, link, source, target) do
    ["XADD", stream, "*", "action", action, "link", link, "source", source, "target", target]
  end
end
