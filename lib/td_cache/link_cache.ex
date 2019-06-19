defmodule TdCache.LinkCache do
  @moduledoc """
  Shared cache for links between entities.
  """
  use GenServer

  alias TdCache.EventStream.Publisher
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
  Counts links for a given key and target type.
  """
  def count(key, target_type) do
    GenServer.call(__MODULE__, {:count, key, target_type})
  end

  @doc """
  Deletes cache entries relating to a given link id.
  """
  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  @doc """
  Deletes all link cache entries relating to a given resource type and id.
  """
  def delete_resource_links(resource_type, resource_id) do
    GenServer.call(__MODULE__, {:delete_resource_links, resource_type, resource_id})
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
  def handle_call({:count, key, target_type}, _from, state) do
    reply = Redis.command(["SCARD", "#{key}:links:#{target_type}"])
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_link(id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete_resource_links, resource_type, resource_id}, _from, state) do
    reply = do_delete_resource_links(resource_type, resource_id)
    {:reply, reply, state}
  end

  ## Private functions

  defp get_link(id) do
    {:ok, tags} = Redis.command(["SMEMBERS", "link:#{id}:tags"])
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

    event = %{
      event: "add_link",
      link: "link:#{id}",
      source: "#{source_type}:#{source_id}",
      target: "#{target_type}:#{target_id}"
    }

    unless source_add_count == 0 do
      {:ok, _event_id} = Publisher.publish(event, "#{source_type}:events")
    end

    unless target_add_count == 0 do
      {:ok, _event_id} = Publisher.publish(event, "#{target_type}:events")
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
           updated_at: updated_at
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
        "updated_at",
        to_string(updated_at)
      ],
      ["SADD", "#{source_type}:#{source_id}:links", "link:#{id}"],
      ["SADD", "#{target_type}:#{target_id}:links", "link:#{id}"],
      ["SADD", "#{source_type}:#{source_id}:links:#{target_type}", "link:#{id}"],
      ["SADD", "#{target_type}:#{target_id}:links:#{source_type}", "link:#{id}"],
      ["SADD", "link:keys", "link:#{id}"]
    ] ++ put_link_tags_commands(link)
  end

  defp put_link_tags_commands(%{tags: []}), do: []

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
    Redis.transaction_pipeline([
      ["DEL", "link:#{id}", "link:#{id}:tags"],
      ["SREM", "link:keys", "link:#{id}"]
    ])
  end

  defp delete_link(id, [source, target]) do
    [source_type, _source_id] = String.split(source, ":")
    [target_type, _target_id] = String.split(target, ":")

    commands = [
      ["SREM", "#{source}:links", "link:#{id}"],
      ["SREM", "#{target}:links", "link:#{id}"],
      ["SREM", "#{source}:links:#{target_type}", "link:#{id}"],
      ["SREM", "#{target}:links:#{source_type}", "link:#{id}"],
      ["DEL", "link:#{id}", "link:#{id}:tags"],
      ["SREM", "link:keys", "link:#{id}"]
    ]

    {:ok, results} = Redis.transaction_pipeline(commands)
    [source_del_count, target_del_count, _, _, _, _] = results

    event = %{
      event: "remove_link",
      link: "link:#{id}",
      source: source,
      target: target
    }

    unless source_del_count == 0 do
      {:ok, _event_id} = Publisher.publish(event, "#{source_type}:events")
    end

    unless target_del_count == 0 do
      {:ok, _event_id} = Publisher.publish(event, "#{target_type}:events")
    end

    {:ok, results}
  end

  defp do_delete_resource_links(source_type, source_id) do
    source_key = "#{source_type}:#{source_id}"
    links_key = "#{source_key}:links"

    [links, "OK"] =
      Redis.transaction_pipeline!([
        ["SMEMBERS", links_key],
        ["RENAME", links_key, "_:#{links_key}"]
      ])

    commands =
      links
      |> Enum.map(&["HMGET", &1, "source", "target"])
      |> Redis.transaction_pipeline!()
      |> Enum.map(fn keys -> Enum.filter(keys, &(&1 != source_key)) end)
      |> Enum.zip(links)
      |> Enum.flat_map(fn {target_keys, link_key} -> Enum.map(target_keys, &{&1, link_key}) end)
      |> Enum.flat_map(&remove_link_commands(&1, source_type, links_key))

    results = Redis.transaction_pipeline!(commands)

    event_ids =
      results
      |> Enum.zip(commands)
      |> publish_bulk_events(source_key)

    {:ok, Enum.count(event_ids), Enum.sum(results)}
  end

  defp remove_link_commands({target_key, link_key}, source_type, links_key) do
    target_type = extract_type(target_key)

    [
      ["SREM", "#{target_key}:links", link_key],
      ["SREM", "#{target_key}:links:#{source_type}", link_key],
      ["DEL", "#{link_key}", "#{link_key}:tags", "#{links_key}:#{target_type}"],
      ["SREM", "link:keys", "#{link_key}"]
    ]
  end

  defp publish_bulk_events(results_zip_commands, source_key) do
    {:ok, event_ids} =
      results_zip_commands
      |> Enum.filter(fn {_, [c | [key | _]]} ->
        c == "SREM" and String.ends_with?(key, ":links")
      end)
      |> Enum.reject(fn {count, _command} -> count == 0 end)
      |> Enum.map(fn {_, [_, target_links_key, link_key]} -> {target_links_key, link_key} end)
      |> Enum.filter(fn {target_links_key, _link_key} ->
        String.ends_with?(target_links_key, ":links")
      end)
      |> Enum.map(fn {target_links_key, link_key} ->
        {extract_type(target_links_key), remove_suffix(target_links_key), link_key}
      end)
      |> Enum.map(&create_event(&1, "remove_link", source_key))
      |> Publisher.publish()

    event_ids
  end

  defp remove_suffix(key, sufix \\ ":links") do
    String.replace_suffix(key, sufix, "")
  end

  defp extract_type(key) do
    key
    |> String.split(":")
    |> hd
  end

  defp create_event({target_type, target_key, link_key}, event, source_key) do
    %{
      event: event,
      link: link_key,
      source: source_key,
      target: target_key,
      stream: "#{target_type}:events"
    }
  end
end
