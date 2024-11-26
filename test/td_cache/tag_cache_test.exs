defmodule TdCache.TagCacheTest do
  use ExUnit.Case

  alias TdCache.Redix
  alias TdCache.TagCache

  doctest TdCache.TagCache

  setup do
    tag = make_tag()

    on_exit(fn ->
      TagCache.delete(tag.id)

      Redix.command([
        "DEL",
        "link:tag:#{tag.id}"
      ])
    end)

    {:ok, tag: tag}
  end

  describe "LinkCache" do
    test "writes a tag entry in redis, and reads it back, delete tag", context do
      tag = context[:tag]
      tag_value = tag.value
      tag_type = Map.get(tag_value, "type", nil)
      tag_target_type = Map.get(tag_value, "target_type", nil)
      tag_expandable = to_string(Map.get(tag_value, "expandable", nil) == "true")

      tag_updated_at = to_string(tag.updated_at)

      assert {:ok, [4, 1]} == TagCache.put(tag)

      {:ok, t} = TagCache.get(tag.id)
      assert t.type == tag_type
      assert t.target_type == tag_target_type
      assert t.expandable == tag_expandable
      assert t.updated_at == tag_updated_at
    end

    test "lists all links", %{tag: %{id: id1} = tag} do
      %{id: id2} = tag2 = make_tag()

      {:ok, _} = TagCache.put(tag)
      {:ok, _} = TagCache.put(tag2)

      assert [_, _] = tags = TagCache.list()
      assert Enum.any?(tags, &(&1.id == "#{id1}"))
      assert Enum.any?(tags, &(&1.id == "#{id2}"))
    end
  end

  defp make_tag(params \\ %{}) do
    %{
      id: System.unique_integer([:positive]),
      value: %{
        "type" => Map.get(params, :type, "foo"),
        "target_type" => Map.get(params, :target_type, "bar"),
        "expandable" => false
      },
      updated_at: DateTime.utc_now()
    }
    |> Map.merge(params)
  end
end
