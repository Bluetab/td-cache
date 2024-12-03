defmodule TdCache.LinkCache do
  @moduledoc """
  Shared cache for links between entities.
  """

  alias TdCache.ConceptCache
  alias TdCache.EventStream.Publisher
  alias TdCache.ImplementationCache
  alias TdCache.IngestCache
  alias TdCache.Link
  alias TdCache.Redix
  alias TdCache.StructureCache

  ## Client API

  @doc """
  Creates cache entries relating to a given link.

  The option `[publish: false]` may be used to prevent events from being published.
  """
  def put(link, opts \\ []) do
    put_link(link, opts)
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
  def list(resource_type, resource_id),
    do: list(resource_type, resource_id, [])

  def list(resource_type, resource_id, opts) when is_list(opts) do
    linked_resources = linked_resources("#{resource_type}:#{resource_id}", opts)
    {:ok, linked_resources}
  end

  @doc """
  Reads linked resources with a given type for a given resource.
  """
  def list(resource_type, resource_id, target_type) when is_binary(target_type),
    do: list(resource_type, resource_id, target_type, [])

  def list(resource_type, resource_id, target_type, opts) when is_binary(target_type) do
    linked_resources = linked_resources("#{resource_type}:#{resource_id}", target_type, opts)
    {:ok, linked_resources}
  end

  @doc """
  Returns a `MapSet` containing the ids of the specified resource type
  which have links to the target resource type.
  """
  def linked_source_ids(source_type, target_type) do
    "#{source_type}:*:links:#{target_type}"
    |> Redix.keys!()
    |> Enum.map(&source_id/1)
    |> Enum.sort()
  end

  defp source_id(key) when is_binary(key) do
    key
    |> String.split(":")
    |> Enum.at(1)
    |> String.to_integer()
  end

  @doc """
  Counts links for a given key and target type.
  """
  def count(key, target_type) do
    Redix.command(["SCARD", "#{key}:links:#{target_type}"])
  end

  @doc """
  Reads distinct link tags for a given key and target type.
  """
  def tags(key, target_type) do
    keys =
      ["SMEMBERS", "#{key}:links:#{target_type}"]
      |> Redix.command!()
      |> Enum.map(&(&1 <> ":tags"))

    case keys do
      [] -> {:ok, []}
      _ -> Redix.command(["SUNION" | keys])
    end
  end

  @doc """
  Deletes cache entries relating to a given link id.

  The option `[publish: false]` may be used to prevent events from being published.
  """
  def delete(id, opts \\ []) do
    delete_link(id, opts)
  end

  @doc """
  Deletes all link cache entries relating to a given resource type and id.
  """
  def delete_resource_links(resource_type, resource_id) do
    do_delete_resource_links(resource_type, resource_id)
  end

  @doc """
  Returns a list of all cached link entries.
  """
  def list_links do
    ["SMEMBERS", "link:keys"]
    |> Redix.command!()
    |> Enum.map(&get_link/1)
  end

  @doc """
  Returns a list of all linked ids with a given prefix.
  """
  def referenced_ids(prefix) do
    list_links()
    |> Enum.flat_map(fn %{source: source, target: target} -> [source, target] end)
    |> Enum.filter(&String.starts_with?(&1, prefix))
    |> Enum.map(&String.replace_leading(&1, prefix, ""))
    |> Enum.uniq()
    |> Enum.map(&String.to_integer/1)
  end

  ## Private functions

  defp get_link("link:" <> id = key) do
    {:ok, tags} = Redix.command(["SMEMBERS", "#{key}:tags"])
    {:ok, map} = Redix.read_map(key)

    case map do
      nil ->
        nil

      _ ->
        link =
          map
          |> Map.put(:tags, tags)
          |> Map.put(:id, id)

        struct(Link, link)
    end
  end

  defp get_link(id) do
    get_link("link:#{id}")
  end

  defp put_link(%{id: id, updated_at: updated_at} = link, opts) do
    last_updated = Redix.command!(["HGET", "link:#{id}", :updated_at])

    link
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_link(last_updated, opts)
  end

  defp put_link(%{updated_at: ts}, ts, _opts), do: {:ok, []}

  defp put_link(
         %{
           id: id,
           source_type: source_type,
           source_id: source_id,
           target_type: target_type,
           target_id: target_id
         } = link,
         _last_updated,
         opts
       ) do
    commands = put_link_commands(link)

    {:ok, results} = Redix.transaction_pipeline(commands)
    source_add_count = Enum.at(results, 2)
    target_add_count = Enum.at(results, 3)

    unless opts[:publish] == false do
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
        "HSET",
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

  defp delete_link(id, opts) do
    {:ok, keys} = Redix.command(["HMGET", "link:#{id}", "source", "target"])
    result = do_delete_link(id, keys, opts)

    unless opts[:publish] == false do
      if did_delete?(result) do
        Publisher.publish(%{
          stream: "link:commands",
          event: "delete_link",
          link_id: id
        })
      end
    end

    result
  end

  def did_delete?({:ok, [count, _]}), do: count > 0
  def did_delete?({:ok, [_, _, _, _, count, _]}), do: count > 0
  def did_delete?(_), do: false

  defp do_delete_link(id, [nil, nil], _opts) do
    Redix.transaction_pipeline([
      ["DEL", "link:#{id}", "link:#{id}:tags"],
      ["SREM", "link:keys", "link:#{id}"]
    ])
  end

  defp do_delete_link(id, [source, target], opts) do
    source_type = extract_type(source)
    target_type = extract_type(target)

    commands = [
      ["SREM", "#{source}:links", "link:#{id}"],
      ["SREM", "#{target}:links", "link:#{id}"],
      ["SREM", "#{source}:links:#{target_type}", "link:#{id}"],
      ["SREM", "#{target}:links:#{source_type}", "link:#{id}"],
      ["DEL", "link:#{id}", "link:#{id}:tags"],
      ["SREM", "link:keys", "link:#{id}"]
    ]

    {:ok, results} = Redix.transaction_pipeline(commands)
    [source_del_count, target_del_count, _, _, _, _] = results

    unless opts[:publish] == false do
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
    end

    {:ok, results}
  end

  defp do_delete_resource_links(source_type, source_id) do
    source_key = "#{source_type}:#{source_id}"
    links_key = "#{source_key}:links"

    [links, "OK"] =
      Redix.transaction_pipeline!([
        ["SMEMBERS", links_key],
        ["RENAME", links_key, "_:#{links_key}"]
      ])

    # TODO: The "_:#{links_key}" key should be deleted after resource links have been removed

    commands =
      links
      |> Enum.map(&["HMGET", &1, "source", "target"])
      |> Redix.transaction_pipeline!()
      |> Enum.map(fn keys -> Enum.filter(keys, &(&1 != source_key)) end)
      |> Enum.zip(links)
      |> Enum.flat_map(fn {target_keys, link_key} -> Enum.map(target_keys, &{&1, link_key}) end)
      |> Enum.flat_map(&remove_link_commands(&1, source_type, links_key))

    results = Redix.transaction_pipeline!(commands)

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

  defp extract_type(key) when is_binary(key) do
    key
    |> String.split(":", parts: 2)
    |> hd()
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

  defp linked_resources(key, target_type, opts) do
    ["SMEMBERS", "#{key}:links:#{target_type}"]
    |> Redix.command!()
    |> get_linked_resources(key, opts)
  end

  defp linked_resources(key, opts) when is_list(opts) do
    ["SMEMBERS", "#{key}:links"]
    |> Redix.command!()
    |> get_linked_resources(key, opts)
  end

  defp get_linked_resources(resources, key, opts) do
    resources
    |> Enum.map(&String.replace_prefix(&1, "link:", ""))
    |> Enum.map(&get_link/1)
    |> Enum.filter(& &1)
    |> maybe_reject_parent_business_concept_links(key, opts)
    |> Enum.flat_map(fn %{id: id, source: source, target: target, tags: tags} ->
      [{source, tags, id}, {target, tags, id}]
    end)
    |> Enum.reject(fn {resource_key, _tags, _id} -> resource_key == key end)
    |> Enum.map(fn {resource_key, tags, id} ->
      {String.split(resource_key, ":", parts: 2), tags, id}
    end)
    |> Enum.map(&read_source(&1, opts))
    |> Enum.filter(& &1)
  end

  defp maybe_reject_parent_business_concept_links(resources, key, opts) do
    if Keyword.get(opts, :childs) do
      Enum.reject(resources, fn
        %{target: ^key, source: "business_concept:" <> _} -> true
        _ -> false
      end)
    else
      resources
    end
  end

  defp read_source({["business_concept", business_concept_id], tags, id}, opts) do
    case ConceptCache.get(business_concept_id, opts) do
      {:ok, nil} ->
        nil

      {:ok, concept} ->
        resource_with_tags(concept, :concept, tags, id)
    end
  end

  defp read_source({["data_structure", structure_id], tags, id}, _opts) do
    case StructureCache.get(structure_id) do
      {:ok, nil} ->
        nil

      {:ok, structure} ->
        resource_with_tags(structure, :data_structure, tags, id)
    end
  end

  defp read_source({["ingest", ingest_id], tags, id}, _opts) do
    case IngestCache.get(ingest_id) do
      {:ok, nil} ->
        nil

      {:ok, ingest} ->
        resource_with_tags(ingest, :ingest, tags, id)
    end
  end

  defp read_source({["implementation_ref", implementation_ref], tags, id}, opts) do
    case ImplementationCache.get(implementation_ref, opts) do
      {:ok, nil} -> nil
      {:ok, implementation} -> resource_with_tags(implementation, :implementation, tags, id)
    end
  end

  defp read_source(_, _), do: nil

  defp resource_with_tags(%{id: resource_id} = resource, type, tags, link_id) do
    resource
    |> Map.put(:resource_id, resource_id)
    |> Map.put(:resource_type, type)
    |> Map.put(:tags, tags)
    |> Map.put(:id, link_id)
  end
end
