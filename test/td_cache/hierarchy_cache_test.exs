defmodule TdCache.HierarchyCacheTest do
  use ExUnit.Case

  alias TdCache.Redix
  alias TdCache.Redix.Stream
  alias TdCache.HierarchyCache

  @name_to_id_key "hierarchies:name_to_id"

  doctest TdCache.HierarchyCache

  setup do
    hierarchies = 0..4 |> Enum.map(fn _ -> random_hierarchy() end) |> Enum.uniq()

    on_exit(fn ->
      hierarchies
      |> Enum.map(& &1.id)
      |> Enum.each(&HierarchyCache.delete/1)

      ConCache.delete(:hierarchies, :all)

      Redix.del!("hierarchy:events")
    end)

    [hierarchies: hierarchies]
  end

  test "put/1 returns Ok", context do
    [hierarchy | _] = context[:hierarchies]
    assert {:ok, [4, 1, 1]} == HierarchyCache.put(hierarchy)
  end

  test "put/1 returns updates only when updated at is changed", %{hierarchies: [hierarchy | _]} do
    assert {:ok, [4, 1, 1]} == HierarchyCache.put(hierarchy)
    assert {:ok, []} == HierarchyCache.put(hierarchy)
  end

  test "put/1 emits an event when a new hierarchy is cached", %{hierarchies: [hierarchy | _]} do
    assert {:ok, [4, 1, 1]} == HierarchyCache.put(hierarchy)

    assert {:ok, [event]} = Stream.read(:redix, ["hierarchy:events"], transform: true)
    assert event.event == "hierarchy_updated"
    assert event.hierarchy == "hierarchy:#{hierarchy.id}"
  end

  test "put/2 suppresses events if publish option is false", %{hierarchies: [hierarchy | _]} do
    assert {:ok, [4, 1, 1]} == HierarchyCache.put(hierarchy, publish: false)
    assert {:ok, []} = Stream.read(:redix, ["hierarchy:events"], transform: true)
  end

  test "put/1 deletes previous names in name_to_id_map", %{
    hierarchies: [%{id: id, name: name} = hierarchy | _]
  } do
    id = to_string(id)

    for name <- ["foo", "bar", "baz"] do
      assert {:ok, 1} = Redix.command(["HSET", @name_to_id_key, name, id])
    end

    assert {:ok, [3, _, _, _]} = HierarchyCache.put(hierarchy)

    #  "hierarchies:name_to_id"
    assert {:ok, [^name, ^id]} = Redix.command(["HGETALL", @name_to_id_key])
  end

  # test "get/1 gets content", context do
  #   [template | _] = context[:templates]
  #   {:ok, _} = HierarchyCache.put(template)
  #   {:ok, content} = HierarchyCache.get(template.id, :content)
  #   assert content == template.content
  # end

  # test "get_by_name gets template", context do
  #   [template | _] = context[:templates]
  #   HierarchyCache.put(template)
  #   {:ok, t} = HierarchyCache.get_by_name(template.name)
  #   assert t.content == template.content
  #   assert t.id == template.id
  #   assert t.name == template.name
  #   assert t.scope == template.scope
  #   assert t.subscope == template.subscope
  #   assert t.updated_at == to_string(template.updated_at)
  # end

  # test "get_by_name gets template, nil subscope converted to empty string", context do
  #   template = context[:template_without_subscope]
  #   HierarchyCache.put(template)
  #   {:ok, t} = HierarchyCache.get_by_name(template.name)
  #   assert t.content == template.content
  #   assert t.id == template.id
  #   assert t.name == template.name
  #   assert t.scope == template.scope
  #   assert t.subscope == ""
  #   assert t.updated_at == to_string(template.updated_at)
  # end

  # test "get_by_name invalid key will return nil" do
  #   assert {:ok, nil} == HierarchyCache.get_by_name("invalid:key")
  # end

  # test "list will return a list of objects", context do
  #   context[:templates]
  #   |> Enum.take(3)
  #   |> Enum.map(&HierarchyCache.put/1)

  #   ConCache.delete(:templates, :all)
  #   {:ok, list} = HierarchyCache.list()
  #   assert length(list) == 3
  # end

  # test "list_by_scope will only return template from the requested scope", context do
  #   templates =
  #     context[:templates]
  #     |> Enum.take(6)
  #     |> Enum.chunk_every(3)
  #     |> Enum.with_index()
  #     |> Enum.flat_map(fn {templates, i} ->
  #       Enum.map(templates, &Map.put(&1, :scope, "scope_#{i}"))
  #     end)

  #   templates
  #   |> Enum.map(&HierarchyCache.put/1)

  #   {:ok, list} = HierarchyCache.list_by_scope("scope_0")
  #   assert length(list) == 3
  # end

  # test "list_by_subsscope will only return template from the requested scope and subscope",
  #      context do
  #   templates =
  #     context[:templates]
  #     |> Enum.take(6)
  #     |> Enum.chunk_every(3)
  #     |> Enum.with_index()
  #     |> Enum.flat_map(fn {templates, i} ->
  #       Enum.map(templates, &Map.put(&1, :subscope, "subscope_#{i}"))
  #     end)

  #   %{id: id, scope: scope} = hd(templates)

  #   templates
  #   |> Enum.map(&HierarchyCache.put/1)

  #   {:ok, [%{id: ^id}]} = HierarchyCache.list_by_subscope(scope, "subscope_0")
  # end

  # test "delete/1 deletes from cache", context do
  #   [template | _] = context[:templates]
  #   HierarchyCache.put(template)
  #   assert {:ok, [1, 1, 1]} == HierarchyCache.delete(template.id)
  #   assert {:ok, nil} == HierarchyCache.get(template.id)
  # end

  # test "fields_by_type!/2 returns fields by type", %{templates: [template | _]} do
  #   %{name: name, scope: scope} = template
  #   HierarchyCache.put(template)
  #   assert HierarchyCache.fields_by_type!(scope, "string") == %{name => ["field"]}
  # end

  defp random_hierarchy do
    hierarchy_id = System.unique_integer([:positive])

    %{
      id: hierarchy_id,
      name: "name_#{hierarchy_id}",
      updated_at: DateTime.utc_now(),
      nodes: [
        %{node_id: 1, hierarchy_id: hierarchy_id, name: "father", parent_id: nil},
        %{node_id: 2, hierarchy_id: hierarchy_id, name: "children_1", parent_id: 1},
        %{node_id: 3, hierarchy_id: hierarchy_id, name: "children_2", parent_id: 1}
      ]
    }
  end
end
