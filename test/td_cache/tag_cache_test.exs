defmodule TdCache.TagCacheTest do
  use ExUnit.Case

  import TdCache.TestOperators
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
      %{
        value: %{
          "type" => type,
          "target_type" => target_type,
          "expandable" => expandable
        },
        updated_at: updated_at
      } = tag = context[:tag]

      assert {:ok, [4, 1]} == TagCache.put(tag)

      {:ok, t} = TagCache.get(tag.id)
      assert t.type == type
      assert t.target_type == target_type
      assert t.expandable == to_string(expandable)
      assert t.updated_at == to_string(updated_at)
    end

    test "lists all links", %{tag: %{id: id1} = tag} do
      %{id: id2} = tag2 = make_tag()

      {:ok, _} = TagCache.put(tag)
      {:ok, _} = TagCache.put(tag2)

      assert tags = TagCache.list()
      assert Enum.map(tags, & &1.id) ||| ["#{id1}", "#{id2}"]
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
