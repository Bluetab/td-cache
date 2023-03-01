defmodule TdCache.HierarchyCacheTest do
  use ExUnit.Case

  alias TdCache.Redix
  alias TdCache.Redix.Stream
  alias TdCache.HierarchyCache

  @name_to_id_key "hierarchies:name_to_id"

  doctest TdCache.HierarchyCache

  setup do
    hierarchy = random_hierarchy()

    on_exit(fn ->
      HierarchyCache.delete(hierarchy.id)
      ConCache.delete(:hierarchies, :all)

      Redix.del!("hierarchy:events")
    end)

    [hierarchy: hierarchy]
  end

  test "put/1 returns Ok", %{hierarchy: hierarchy} do
    assert {:ok, [4, 1, 1]} == HierarchyCache.put(hierarchy)
  end

  test "put/1 returns updates only when updated at is changed", %{hierarchy: hierarchy} do
    assert {:ok, [4, 1, 1]} == HierarchyCache.put(hierarchy)
    assert {:ok, []} == HierarchyCache.put(hierarchy)
  end

  test "put/1 emits an event when a new hierarchy is cached", %{hierarchy: hierarchy} do
    assert {:ok, [4, 1, 1]} == HierarchyCache.put(hierarchy)

    assert {:ok, [event]} = Stream.read(:redix, ["hierarchy:events"], transform: true)
    assert event.event == "hierarchy_updated"
    assert event.hierarchy == "hierarchy:#{hierarchy.id}"
  end

  test "put/2 suppresses events if publish option is false", %{hierarchy: hierarchy} do
    assert {:ok, [4, 1, 1]} == HierarchyCache.put(hierarchy, publish: false)
    assert {:ok, []} = Stream.read(:redix, ["hierarchy:events"], transform: true)
  end

  test "put/1 deletes previous names in name_to_id_map", %{
    hierarchy: %{id: id, name: name} = hierarchy
  } do
    id = to_string(id)

    for name <- ["foo", "bar", "baz"] do
      assert {:ok, 1} = Redix.command(["HSET", @name_to_id_key, name, id])
    end

    assert {:ok, [3, _, _, _]} = HierarchyCache.put(hierarchy)

    assert {:ok, [^name, ^id]} = Redix.command(["HGETALL", @name_to_id_key])
  end

  test "get/1 gets nodes", %{hierarchy: %{id: id, nodes: nodes} = hierarchy} do
    {:ok, _} = HierarchyCache.put(hierarchy)

    {:ok, cache_nodes} = HierarchyCache.get(id, :nodes)

    atom_nodes = nodes_to_atom(cache_nodes)

    assert atom_nodes == nodes
  end

  test "get_by_name gets hierarchy", %{
    hierarchy: %{id: id, name: name, nodes: nodes} = hierarchy
  } do
    HierarchyCache.put(hierarchy)
    {:ok, h} = HierarchyCache.get_by_name(name)

    atom_nodes = nodes_to_atom(h.nodes)

    assert atom_nodes == nodes
    assert h.id == id
    assert h.name == name
    assert h.updated_at == to_string(hierarchy.updated_at)
  end

  test "get_by_name invalid key will return nil" do
    assert {:ok, nil} == HierarchyCache.get_by_name("invalid:key")
  end

  test "list will return a list of objects" do
    hierarchies =
      Enum.map(0..4, fn _ ->
        hierarchy = random_hierarchy()
        HierarchyCache.put(hierarchy)
        hierarchy
      end)

    ConCache.delete(:hierarchies, :all)

    {:ok, list} = HierarchyCache.list()

    assert length(list) == 5

    hierarchies
    |> Enum.map(& &1.id)
    |> Enum.each(&HierarchyCache.delete/1)
  end

  test "delete/1 deletes from cache", %{hierarchy: hierarchy} do
    %{id: id} = hierarchy
    HierarchyCache.put(hierarchy)
    assert {:ok, [1, 1, 1]} == HierarchyCache.delete(id)
    assert {:ok, nil} == HierarchyCache.get(id)
  end

  defp nodes_to_atom(nodes) do
    Enum.map(nodes, fn node ->
      node
      |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)
      |> Map.new()
    end)
  end

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
