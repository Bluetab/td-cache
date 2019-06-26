defmodule TdCache.LinkCache do
  @moduledoc """
  Shared cache for links between entities.
  """
  alias TdCache.ConceptCache
  alias TdCache.EventStream.Publisher
  alias TdCache.FieldCache
  alias TdCache.Redix, as: Redis
  alias TdCache.StructureCache

  ## Client API

  @doc """
  Creates cache entries relating to a given link.
  """
  def put(link) do
    put_link(link)
  end

  @doc """
  Reads a cache entries relating to a given link id.
  """
  def get(id) do
    reply = get_link(id)
    {:ok, reply}
  end

  @doc """
  Reads linked resources for a given resource.
  """
  def list(resource_type, resource_id) do
    linked_resources = linked_resources("#{resource_type}:#{resource_id}")
    {:ok, linked_resources}
  end

  @doc """
  Reads linked resources with a given type for a given resource.
  """
  def list(resource_type, resource_id, target_type) do
    linked_resources = linked_resources("#{resource_type}:#{resource_id}", target_type)
    {:ok, linked_resources}
  end

  @doc """
  Counts links for a given key and target type.
  """
  def count(key, target_type) do
    Redis.command(["SCARD", "#{key}:links:#{target_type}"])
  end

  @doc """
  Deletes cache entries relating to a given link id.
  """
  def delete(id) do
    delete_link(id)
  end

  @doc """
  Deletes all link cache entries relating to a given resource type and id.
  """
  def delete_resource_links(resource_type, resource_id) do
    do_delete_resource_links(resource_type, resource_id)
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

  defp put_link(%{id: id, updated_at: updated_at} = link) do
    last_updated = Redis.command!(["HGET", "link:#{id}", :updated_at])

    link
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_link(last_updated)
  end

  defp put_link(%{updated_at: ts}, ts), do: {:ok, []}

  defp put_link(
         %{
           id: id,
           source_type: source_type,
           source_id: source_id,
           target_type: target_type,
           target_id: target_id
         } = link,
         _last_updated
       ) do
    commands = put_link_commands(link)

    {:ok, results} = Redis.transaction_pipeline(commands)
    source_add_count = Enum.at(results, 2)
    target_add_count = Enum.at(results, 3)

    # Publish events if link count has incremented
    [source_add_count, target_add_count]
    |> Enum.zip(["#{source_type}:events", "#{target_type}:events"])
    |> Enum.flat_map(fn {n, stream} ->
      conditional_events(
        n > 0,
        %{
          stream: stream,
          event: "add_link",
          link: "link:#{id}",
          source: "#{source_type}:#{source_id}",
          target: "#{target_type}:#{target_id}"
        }
      )
    end)
    |> Enum.uniq()
    |> Publisher.publish()

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
        "#{updated_at}"
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

    # Publish events if link count has decremented
    [source_del_count, target_del_count]
    |> Enum.zip(["#{source_type}:events", "#{target_type}:events"])
    |> Enum.flat_map(fn {n, stream} ->
      conditional_events(n > 0, %{
        stream: stream,
        event: "remove_link",
        link: "link:#{id}",
        source: source,
        target: target
      })
    end)
    |> Enum.uniq()
    |> Publisher.publish()

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

    # TODO: The "_:#{links_key}" key should be deleted after resource links have been removed

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

  defp conditional_events(false, _), do: []
  defp conditional_events(_true, e), do: [e]

  defp linked_resources(key, target_type) do
    ["SMEMBERS", "#{key}:links:#{target_type}"]
    |> Redis.command!()
    |> get_linked_resources(key)
  end

  defp linked_resources(key) do
    ["SMEMBERS", "#{key}:links"]
    |> Redis.command!()
    |> get_linked_resources(key)
  end

  defp get_linked_resources(resources, key) do
    resources
    |> Enum.map(&String.replace_prefix(&1, "link:", ""))
    |> Enum.map(&get_link/1)
    |> Enum.filter(& &1)
    |> Enum.flat_map(fn %{source: source, target: target, tags: tags} ->
      [{source, tags}, {target, tags}]
    end)
    |> Enum.reject(fn {resource_key, _tags} -> resource_key == key end)
    |> Enum.map(fn {resource_key, tags} -> {String.split(resource_key, ":"), tags} end)
    |> Enum.map(&read_source/1)
    |> Enum.filter(& &1)
  end

  defp read_source({["business_concept", business_concept_id], tags}) do
    case ConceptCache.get(business_concept_id) do
      {:ok, nil} ->
        nil

      {:ok, concept} ->
        resource_with_tags(concept, :concept, tags)
    end
  end

  defp read_source({["data_field", data_field_id], tags}) do
    case FieldCache.get(data_field_id) do
      {:ok, nil} ->
        nil

      {:ok, field} ->
        resource_with_tags(field, :data_field, tags)
    end
  end

  defp read_source({["data_structure", structure_id], tags}) do
    case StructureCache.get(structure_id) do
      {:ok, nil} ->
        nil

      {:ok, structure} ->
        resource_with_tags(structure, :data_structure, tags)
    end
  end

  defp read_source(_), do: []

  defp resource_with_tags(resource, type, tags) do
    resource
    |> Map.put(:resource_type, type)
    |> Map.put(:tags, tags)
  end
end
