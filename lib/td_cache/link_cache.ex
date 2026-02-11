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

  @default_batch_size 100

  ## Client API

  @doc """
  Creates cache entries relating to a given link.

  The option `[publish: false]` may be used to prevent events from being published.
  """
  def put(link, opts \\ []) do
    put_link(link, opts)
  end

  @doc """
  Creates cache entries for multiple links in a batch.

  The option `[publish: false]` may be used to prevent events from being published.
  The option `[batch_size: size]` can be used to control the batch size.
  """
  def put_many(links, opts \\ []) when is_list(links) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    publish = Keyword.get(opts, :publish, true)

    {successful, failed} =
      links
      |> Enum.chunk_every(batch_size)
      |> Enum.map(&process_batch(&1, publish))
      |> Enum.reduce({[], []}, fn {batch_successful, batch_failed},
                                  {all_successful, all_failed} ->
        {all_successful ++ batch_successful, all_failed ++ batch_failed}
      end)

    {:ok, successful, failed}
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

  def list_rand_links(resource_type, resource_id, target_type, count \\ 10) do
    key = "#{resource_type}:#{resource_id}"

    linked_resources =
      ["SRANDMEMBER", "#{key}:links:#{target_type}", count]
      |> Redix.command!()
      |> get_linked_resources(key, [])

    {:ok, linked_resources}
  end

  ## Private functions

  defp get_link("link:" <> id = key) do
    with {:ok, tags} <- Redix.command(["SMEMBERS", "#{key}:tags"]),
         {:ok, map} when not is_nil(map) <- Redix.read_map(key) do
      link =
        map
        |> Map.put(:tags, tags)
        |> Map.put(:id, id)

      struct(Link, link)
    else
      {:ok, nil} -> nil
      _ -> nil
    end
  end

  defp get_link(id) do
    get_link("link:#{id}")
  end

  defp put_link(%{id: id, updated_at: updated_at} = link, opts) do
    last_updated = Redix.command!(["HGET", "link:#{id}", :updated_at])

    link
    |> Map.put(:updated_at, "#{updated_at}")
    |> validate_origin
    |> put_link(last_updated, opts)
  end

  defp put_link(%{updated_at: ts}, ts, _opts), do: {:ok, []}

  defp put_link(
         link,
         _last_updated,
         opts
       ) do
    commands = put_link_commands(link)

    {:ok, results} = transaction_pipeline(commands)
    source_add_count = Enum.at(results, 2)
    target_add_count = Enum.at(results, 3)

    unless opts[:publish] == false do
      publish_link_events(source_add_count, target_add_count, link)
    end

    {:ok, results}
  end

  defp publish_link_events(source_add_count, target_add_count, link) do
    %{
      id: id,
      source_type: source_type,
      source_id: source_id,
      target_type: target_type,
      target_id: target_id
    } = link

    events =
      [
        {source_add_count, "#{source_type}:events"},
        {target_add_count, "#{target_type}:events"}
      ]
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

    Publisher.publish(events)
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
    ]
    |> maybe_link_tags_commands(link)
    |> maybe_origin_field(link)
  end

  defp process_batch(links, publish) do
    {commands, link_command_map, ordered_link_ids} = prepare_batch_commands(links)

    if length(commands) != [] do
      process_batch_with_commands(links, link_command_map, ordered_link_ids, commands, publish)
    else
      {links, []}
    end
  end

  defp process_batch_with_commands(links, link_command_map, ordered_link_ids, commands, publish) do
    case transaction_pipeline(commands) do
      {:ok, results} ->
        {successful_links, failed_links} =
          process_batch_results_dynamic_by_order(ordered_link_ids, link_command_map, results)

        if publish and length(successful_links) != [] do
          publish_batch_events_dynamic(successful_links, link_command_map, results)
        end

        {successful_links, failed_links}

      {:error, reason} ->
        failed_links =
          links
          |> Enum.map(&Map.put(&1, :error_reason, reason))

        {[], failed_links}
    end
  end

  defp transaction_pipeline(commands) do
    redix_module = Application.get_env(:td_cache, :redix_module, Redix)

    case redix_module do
      nil ->
        Redix.transaction_pipeline(commands)

      {:module, actual_module, _, _} when is_atom(actual_module) ->
        actual_module.transaction_pipeline(commands)

      module when is_atom(module) ->
        module.transaction_pipeline(commands)
    end
  end

  defp prepare_batch_commands(links) do
    Enum.reduce(links, {[], %{}, []}, fn link, {all_commands, link_map, ordered_link_ids} ->
      case prepare_single_link_commands(link) do
        {:ok, cmds} when cmds != [] ->
          new_link_map =
            Map.put(link_map, link.id, %{
              link: link,
              commands: cmds
            })

          {all_commands ++ cmds, new_link_map, ordered_link_ids ++ [link.id]}

        {:ok, []} ->
          new_link_map = Map.put(link_map, link.id, %{link: link, commands: [], skipped: true})
          {all_commands, new_link_map, ordered_link_ids ++ [link.id]}

        {:error, reason} ->
          new_link_map =
            Map.put(link_map, link.id, %{
              link: Map.put(link, :error_reason, reason),
              commands: [],
              invalid: true
            })

          {all_commands, new_link_map, ordered_link_ids ++ [link.id]}
      end
    end)
    |> then(fn {commands, link_map, ordered_ids} -> {commands, link_map, ordered_ids} end)
  end

  defp prepare_single_link_commands(
         %{
           id: id,
           source_type: _,
           source_id: _,
           target_type: _,
           target_id: _,
           updated_at: updated_at
         } = link
       )
       when is_integer(id) do
    case Redix.command(["HGET", "link:#{id}", :updated_at]) do
      {:ok, last_updated} ->
        if last_updated == "#{updated_at}" do
          {:ok, []}
        else
          {:ok, put_link_commands(link)}
        end

      {:error, _reason} ->
        {:ok, put_link_commands(link)}
    end
  end

  defp prepare_single_link_commands(_link) do
    {:error, :invalid_link_structure}
  end

  defp process_batch_results_dynamic_by_order(ordered_link_ids, link_command_map, results) do
    {successful_links, failed_links} =
      Enum.reduce(ordered_link_ids, {[], []}, fn link_id, {successful, failed} ->
        link_info = Map.get(link_command_map, link_id)
        link = link_info.link

        handle_link_result(
          link_info,
          link,
          successful,
          failed,
          results,
          ordered_link_ids,
          link_command_map
        )
      end)

    {successful_links, failed_links}
  end

  defp handle_link_result(
         %{skipped: true},
         link,
         successful,
         failed,
         _results,
         _ordered_link_ids,
         _link_command_map
       ) do
    {[link | successful], failed}
  end

  defp handle_link_result(
         %{invalid: true} = link_info,
         _link,
         successful,
         failed,
         _results,
         _ordered_link_ids,
         _link_command_map
       ) do
    {successful, [link_info.link | failed]}
  end

  defp handle_link_result(
         %{commands: commands} = link_info,
         link,
         successful,
         failed,
         results,
         ordered_link_ids,
         link_command_map
       )
       when is_list(commands) and length(commands) != [] do
    {start_idx, end_idx} =
      find_command_range_for_link_by_order(link_info.link.id, ordered_link_ids, link_command_map)

    link_results = Enum.slice(results, start_idx, end_idx - start_idx)

    if all_commands_successful?(link_results) do
      {[link | successful], failed}
    else
      failed_link = Map.put(link, :error_reason, :partial_failure)
      {successful, [failed_link | failed]}
    end
  end

  defp handle_link_result(
         _link_info,
         link,
         successful,
         failed,
         _results,
         _ordered_link_ids,
         _link_command_map
       ) do
    {[link | successful], failed}
  end

  defp find_command_range_for_link_by_order(link_id, ordered_link_ids, link_command_map) do
    {start_idx, _found} =
      Enum.reduce_while(ordered_link_ids, {0, false}, fn current_link_id, {idx, _found} ->
        current_link_commands =
          Map.get(link_command_map, current_link_id) |> Map.get(:commands, [])

        if current_link_id == link_id do
          {:halt, {idx, true}}
        else
          {:cont, {idx + length(current_link_commands), false}}
        end
      end)

    link_commands = Map.get(link_command_map, link_id) |> Map.get(:commands, [])
    end_idx = start_idx + length(link_commands)
    {start_idx, end_idx}
  end

  defp all_commands_successful?(results) do
    Enum.all?(results, &command_successful?/1)
  end

  defp command_successful?(result) do
    case result do
      n when is_integer(n) -> n >= 0
      "OK" -> true
      {:ok, _} -> true
      true -> true
      :ok -> true
      _ -> false
    end
  end

  defp publish_batch_events_dynamic(successful_links, link_command_map, results) do
    events =
      successful_links
      |> Enum.flat_map(fn link ->
        link_info = Map.get(link_command_map, link.id)

        if link_info && !Map.get(link_info, :skipped, false) do
          extract_sadd_events_for_link(link, link_info, results, link_command_map)
        else
          []
        end
      end)
      |> Enum.uniq()

    if length(events) != [] do
      Publisher.publish(events)
    end
  end

  defp extract_sadd_events_for_link(link, link_info, all_results, link_command_map) do
    %{
      id: id,
      source_type: source_type,
      source_id: source_id,
      target_type: target_type,
      target_id: target_id
    } = link

    {start_idx, _end_idx} = calculate_command_indices_for_link(link.id, link_command_map)

    link_info.commands
    |> Enum.with_index()
    |> Enum.flat_map(fn {cmd, cmd_idx} ->
      case cmd do
        ["SADD", key, "link:" <> _] ->
          result_idx = start_idx + cmd_idx
          result = Enum.at(all_results, result_idx, 0)

          cond do
            key == "#{source_type}:#{source_id}:links" and command_successful?(result) ->
              [
                create_add_link_event(
                  source_type,
                  source_type,
                  source_id,
                  target_type,
                  target_id,
                  id
                )
              ]

            key == "#{target_type}:#{target_id}:links" and command_successful?(result) ->
              [
                create_add_link_event(
                  target_type,
                  source_type,
                  source_id,
                  target_type,
                  target_id,
                  id
                )
              ]

            true ->
              []
          end

        _ ->
          []
      end
    end)
  end

  defp calculate_command_indices_for_link(target_link_id, link_command_map) do
    {start_idx, _found} =
      Enum.reduce_while(Map.keys(link_command_map), {0, false}, fn current_link_id,
                                                                   {idx, _found} ->
        current_link_commands =
          Map.get(link_command_map, current_link_id) |> Map.get(:commands, [])

        if current_link_id == target_link_id do
          {:halt, {idx, true}}
        else
          {:cont, {idx + length(current_link_commands), false}}
        end
      end)

    target_link_commands = Map.get(link_command_map, target_link_id) |> Map.get(:commands, [])
    end_idx = start_idx + length(target_link_commands)
    {start_idx, end_idx}
  end

  defp create_add_link_event(stream_type, source_type, source_id, target_type, target_id, link_id) do
    %{
      stream: "#{stream_type}:events",
      event: "add_link",
      link: "link:#{link_id}",
      source: "#{source_type}:#{source_id}",
      target: "#{target_type}:#{target_id}"
    }
  end

  defp validate_origin(%{origin: origin} = link) when is_binary(origin),
    do: link

  defp validate_origin(%{origin: _} = link),
    do: Map.delete(link, :origin)

  defp validate_origin(link), do: link

  defp maybe_link_tags_commands(commands, %{tags: []}), do: commands

  defp maybe_link_tags_commands(commands, %{id: id, tags: tags}) do
    commands ++ [["SADD", "link:#{id}:tags"] ++ tags]
  end

  defp maybe_link_tags_commands(commands, _), do: commands

  defp maybe_origin_field([del_command, hset_command | tail_commands], %{origin: origin})
       when is_binary(origin) do
    [del_command, hset_command ++ ["origin", origin] | tail_commands]
  end

  defp maybe_origin_field(commands, _), do: commands

  defp delete_link(id, opts) do
    with {:ok, keys} <- Redix.command(["HMGET", "link:#{id}", "source", "target"]),
         result <- do_delete_link(id, keys, opts) do
      unless opts[:publish] == false do
        publish_delete_event_if_needed(result, id)
      end

      result
    end
  end

  defp publish_delete_event_if_needed(result, id) do
    if did_delete?(result) do
      Publisher.publish(%{
        stream: "link:commands",
        event: "delete_link",
        link_id: id
      })
    end
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
      publish_delete_events(
        source_del_count,
        target_del_count,
        source_type,
        target_type,
        source,
        target,
        id
      )
    end

    {:ok, results}
  end

  defp publish_delete_events(
         source_del_count,
         target_del_count,
         source_type,
         target_type,
         source,
         target,
         id
       ) do
    events =
      [
        {source_del_count, "#{source_type}:events"},
        {target_del_count, "#{target_type}:events"}
      ]
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

    Publisher.publish(events)
  end

  defp do_delete_resource_links(source_type, source_id) do
    source_key = "#{source_type}:#{source_id}"
    links_key = "#{source_key}:links"

    [links, "OK"] =
      Redix.transaction_pipeline!([
        ["SMEMBERS", links_key],
        ["RENAME", links_key, "_:#{links_key}"]
      ])

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
      |> Enum.filter(&srem_link_command?/1)
      |> Enum.reject(fn {count, _command} -> count == 0 end)
      |> Enum.map(fn {_, [_, target_links_key, link_key]} -> {target_links_key, link_key} end)
      |> Enum.filter(&valid_links_key?/1)
      |> Enum.map(&create_remove_event(&1, source_key))
      |> Publisher.publish()

    event_ids
  end

  defp srem_link_command?({_, ["SREM", key | _]}) do
    String.ends_with?(key, ":links")
  end

  defp srem_link_command?(_), do: false

  defp valid_links_key?({key, _}) do
    String.ends_with?(key, ":links")
  end

  defp create_remove_event({target_links_key, link_key}, source_key) do
    target_type = extract_type(target_links_key)
    target_key = String.replace_suffix(target_links_key, ":links", "")

    %{
      event: "remove_link",
      link: link_key,
      source: source_key,
      target: target_key,
      stream: "#{target_type}:events"
    }
  end

  defp extract_type(key) when is_binary(key) do
    key
    |> String.split(":", parts: 2)
    |> hd()
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
    |> Enum.flat_map(fn %{id: id, source: source, target: target, tags: tags, origin: origin} ->
      [{source, tags, id, origin}, {target, tags, id, origin}]
    end)
    |> Enum.reject(fn {resource_key, _tags, _id, _origin} -> resource_key == key end)
    |> Enum.map(fn {resource_key, tags, id, origin} ->
      {String.split(resource_key, ":", parts: 2), tags, id, origin}
    end)
    |> Enum.map(&read_source(&1, opts))
    |> Enum.filter(& &1)
  end

  defp maybe_reject_parent_business_concept_links(resources, key, opts) do
    if Keyword.get(opts, :without_parent_business_concepts) do
      Enum.reject(resources, fn
        %{target: ^key, source: "business_concept:" <> _} -> true
        _ -> false
      end)
    else
      resources
    end
  end

  defp read_source({["business_concept", business_concept_id], tags, id, origin}, opts) do
    case ConceptCache.get(business_concept_id, opts) do
      {:ok, nil} -> nil
      {:ok, concept} -> resource_with_tags(concept, :concept, tags, id, origin)
    end
  end

  defp read_source({["data_structure", structure_id], tags, id, origin}, _opts) do
    case StructureCache.get(structure_id) do
      {:ok, nil} -> nil
      {:ok, structure} -> resource_with_tags(structure, :data_structure, tags, id, origin)
    end
  end

  defp read_source({["ingest", ingest_id], tags, id, origin}, _opts) do
    case IngestCache.get(ingest_id) do
      {:ok, nil} -> nil
      {:ok, ingest} -> resource_with_tags(ingest, :ingest, tags, id, origin)
    end
  end

  defp read_source({["implementation_ref", implementation_ref], tags, id, origin}, opts) do
    case ImplementationCache.get(implementation_ref, opts) do
      {:ok, nil} ->
        nil

      {:ok, implementation} ->
        resource_with_tags(implementation, :implementation, tags, id, origin)
    end
  end

  defp read_source(_, _), do: nil

  defp resource_with_tags(%{id: resource_id} = resource, type, tags, link_id, origin) do
    resource
    |> Map.put(:resource_id, resource_id)
    |> Map.put(:resource_type, type)
    |> Map.put(:tags, tags)
    |> Map.put(:id, link_id)
    |> Map.put(:origin, origin)
  end
end
