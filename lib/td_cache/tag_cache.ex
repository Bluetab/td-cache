defmodule TdCache.TagCache do
  @moduledoc """
  Shared cache for tags links.
  """

  alias TdCache.Redix
  alias TdCache.Tag

  @doc """
  Creates cache entries relating to a given tag.
  """
  def put(tag), do: put_tag(tag)

  defp put_tag(%{id: id, updated_at: updated_at} = tag) do
    last_updated = Redix.command!(["HGET", "link:tag:#{id}", :updated_at])

    tag
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_tag(last_updated)
  end

  defp put_tag(%{updated_at: ts}, ts), do: {:ok, []}

  defp put_tag(tag, _last_updated), do: Redix.transaction_pipeline(put_tag_commands(tag))

  defp put_tag_commands(%{
         id: id,
         value: %{"type" => type, "target_type" => target_type, "expandable" => expandable},
         updated_at: updated_at
       }) do
    [
      [
        "HSET",
        "link:tag:#{id}",
        "type",
        "#{type}",
        "target_type",
        "#{target_type}",
        "expandable",
        "#{expandable}",
        "updated_at",
        "#{updated_at}"
      ],
      ["SADD", "link:tag:keys", "link:tag:#{id}"]
    ]
  end

  defp put_tag_commands(%{
         id: id,
         value: %{"type" => type, "target_type" => target_type},
         updated_at: updated_at
       }) do
    [
      [
        "HSET",
        "link:tag:#{id}",
        "type",
        "#{type}",
        "target_type",
        "#{target_type}",
        "updated_at",
        "#{updated_at}"
      ],
      ["SADD", "link:tag:keys", "link:tag:#{id}"]
    ]
  end

  @doc """
  Reads a cache entries relating to a given tag id.
  """
  def get(id) do
    reply = get_tag(id)
    {:ok, reply}
  end

  defp get_tag("link:tag:" <> id = key) do
    {:ok, map} = Redix.read_map(key)

    case map do
      nil ->
        nil

      _ ->
        Map.merge(%Tag{id: id}, map)
    end
  end

  defp get_tag(id), do: get_tag("link:tag:#{id}")

  def list do
    ["SMEMBERS", "link:tag:keys"]
    |> Redix.command!()
    |> Enum.map(&get_tag/1)
    |> Enum.filter(& &1)
  end

  def delete(id), do: do_delete_tag(id, Redix.command!(["EXISTS", "link:tag:#{id}"]) == 1)

  defp do_delete_tag(id, true) do
    Redix.transaction_pipeline([
      ["DEL", "link:tag:#{id}"],
      ["SREM", "link:tag:keys", "link:#{id}"]
    ])
  end

  defp do_delete_tag(_id, false), do: nil
end
