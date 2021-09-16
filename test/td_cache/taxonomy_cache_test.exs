defmodule TdCache.TaxonomyCacheTest do
  use ExUnit.Case

  alias TdCache.AclCache
  alias TdCache.Redix
  alias TdCache.Redix.Stream
  alias TdCache.TaxonomyCache

  @role "test_role"

  doctest TdCache.TaxonomyCache

  setup do
    root = random_domain()
    parent = random_domain() |> Map.put(:parent_ids, [root.id])
    domain = random_domain() |> Map.put(:parent_ids, [parent.id, root.id])

    root = Map.put(root, :descendent_ids, [parent.id, domain.id])
    parent = Map.put(parent, :descendent_ids, [domain.id])

    on_exit(fn -> Redix.del!(["domain:*", "domains:*"]) end)

    {:ok, root: root, parent: parent, domain: domain}
  end

  test "put_domain returns OK", %{domain: domain} do
    assert {:ok, [5, 1, 1, 1, 0, 0, 0]} = TaxonomyCache.put_domain(domain)
    assert {:ok, events} = Stream.read(:redix, ["domain:events"], transform: true)
    assert [%{event: "domain_created"}] = events
  end

  test "put_domain forces refresh if specified", %{domain: domain} do
    assert {:ok, [5, 1, 1, 1, 0, 0, 0]} = TaxonomyCache.put_domain(domain)
    assert {:ok, []} = TaxonomyCache.put_domain(domain)
    assert {:ok, [0, 0, 0, 0, 0, 0, 0]} = TaxonomyCache.put_domain(domain, force: true)
  end

  test "put_domain invalidates local cache", %{domain: %{id: id} = domain} do
    ConCache.put(:taxonomy, {:id, id}, :foo)
    ConCache.put(:taxonomy, {:parent, id}, :foo)
    ConCache.put(:taxonomy, {:descendents, id}, :foo)
    assert ConCache.get(:taxonomy, {:id, id})
    assert ConCache.get(:taxonomy, {:parent, id})
    assert ConCache.get(:taxonomy, {:descendents, id})
    TaxonomyCache.put_domain(domain)
    refute ConCache.get(:taxonomy, {:id, id})
    refute ConCache.get(:taxonomy, {:parent, id})
    refute ConCache.get(:taxonomy, {:descendents, id})
  end

  test "get_parent_ids with self returns parent ids including domain_id", %{domain: domain} do
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_parent_ids(domain.id) == [domain.id | domain.parent_ids]
  end

  test "get_parent_ids with refresh opt forces to update in-memory info", %{domain: domain} do
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_parent_ids(domain.id, false, refresh: true) == domain.parent_ids
  end

  test "get_parent_ids without self returns parent ids excluding domain_id", %{domain: domain} do
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_parent_ids(domain.id, false) == domain.parent_ids
  end

  test "get_parent_ids when domain has no parents returns an empty list", %{domain: domain} do
    domain = Map.put(domain, :parent_ids, [])
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_parent_ids(domain.id, false) == []
  end

  test "get_descendent_ids with self returns descendent ids including domain_id", %{root: domain} do
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_descendent_ids(domain.id) == [domain.id | domain.descendent_ids]
  end

  test "get_descendent_ids with refresh opt forces to update in-memory info", %{root: domain} do
    TaxonomyCache.put_domain(domain)

    assert TaxonomyCache.get_descendent_ids(domain.id, false, refresh: true) ==
             domain.descendent_ids
  end

  test "get_descendent_ids without self returns descendent ids excluding domain_id", %{
    parent: domain
  } do
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_parent_ids(domain.id, false) == domain.parent_ids
  end

  test "get_descendent_ids when domain has no descendents returns an empty list", %{
    domain: domain
  } do
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_descendent_ids(domain.id, false) == []
  end

  test "get_name returns name", %{domain: domain} do
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_name(domain.id) == domain.name
  end

  test "delete_domain deletes the domain from cache", %{domain: domain} do
    TaxonomyCache.put_domain(domain)
    TaxonomyCache.delete_domain(domain.id)
    refute Redix.exists?("domain:#{domain.id}")
  end

  test "delete_domain invalidates local cache" do
    ConCache.put(:taxonomy, {:id, 123}, :foo)
    ConCache.put(:taxonomy, {:parent, 123}, :foo)
    assert ConCache.get(:taxonomy, {:id, 123})
    assert ConCache.get(:taxonomy, {:parent, 123})
    TaxonomyCache.delete_domain(123)
    refute ConCache.get(:taxonomy, {:id, 123})
    refute ConCache.get(:taxonomy, {:parent, 123})
  end

  test "get_domain_name_to_id_map returns a map with names as keys and ids as values",
       %{root: root, parent: parent, domain: domain} do
    domains = [root, parent, domain]

    Enum.map(domains, &TaxonomyCache.put_domain(&1))

    map = TaxonomyCache.get_domain_name_to_id_map()

    domains
    |> Enum.all?(&Map.has_key?(map, &1.name))
    |> assert
  end

  test "get_domain_external_id_to_id_map returns a map with names as keys and ids as values",
       %{root: root, parent: parent, domain: domain} do
    domains = [root, parent, domain]

    Enum.map(domains, &TaxonomyCache.put_domain(&1))

    map = TaxonomyCache.get_domain_external_id_to_id_map()

    domains
    |> Enum.all?(&(Map.get(map, &1.external_id) == &1.id))
    |> assert
  end

  test "get_domain_ids returns a list with all domain ids",
       %{root: root, parent: parent, domain: domain} do
    domains = [root, parent, domain]

    ids =
      domains
      |> Enum.map(& &1.id)
      |> Enum.sort()

    Enum.each(domains, &TaxonomyCache.put_domain(&1))

    assert Enum.sort(TaxonomyCache.get_domain_ids()) == ids
  end

  test "get_deleted_domain_ids returns a list with all deleted domain ids",
       %{root: root, parent: parent, domain: domain} do
    domains = [root, parent]
    deleted = [domain]

    ids =
      deleted
      |> Enum.map(& &1.id)
      |> Enum.sort()

    Enum.each(domains ++ deleted, &TaxonomyCache.put_domain(&1))
    Enum.each(deleted, fn %{id: id} -> TaxonomyCache.delete_domain(id) end)

    assert Enum.sort(TaxonomyCache.get_deleted_domain_ids()) == ids
  end

  test "domain_map/0 returns a map of domains", %{
    root: %{id: id1} = root,
    parent: %{id: id2} = parent,
    domain: %{id: id3} = domain
  } do
    Enum.map([root, parent, domain], &TaxonomyCache.put_domain/1)

    assert %{} = map = TaxonomyCache.domain_map()

    assert %{
             id: ^id1,
             parent_ids: [^id1],
             descendent_ids: [^id1, ^id2, ^id3],
             external_id: _,
             name: _
           } = map[id1]

    assert %{
             id: ^id2,
             parent_ids: [^id2, ^id1],
             descendent_ids: [^id2, ^id3],
             external_id: _,
             name: _
           } = map[id2]

    assert %{
             id: ^id3,
             parent_ids: [^id3, ^id2, ^id1],
             descendent_ids: [^id3],
             external_id: _,
             name: _
           } = map[id3]
  end

  describe "has_role?/4" do
    setup %{
      root: root,
      parent: %{id: id2} = parent,
      domain: domain
    } do
      user_id = System.unique_integer([:positive])
      Enum.each([root, parent, domain], &TaxonomyCache.put_domain/1)
      AclCache.set_acl_role_users("domain", id2, @role, [user_id])
      on_exit(fn -> AclCache.delete_acl_roles("domain", id2) end)
      [user_id: user_id]
    end

    test "returns true if a user_id has a role in a domain or its parents", %{
      root: %{id: id1},
      parent: %{id: id2},
      domain: %{id: id3},
      user_id: user_id
    } do
      refute TaxonomyCache.has_role?(id1, @role, user_id)
      assert TaxonomyCache.has_role?(id2, @role, user_id)
      assert TaxonomyCache.has_role?(id3, @role, user_id)
    end
  end

  defp random_domain(params \\ %{}) do
    id = Map.get(params, :id, System.unique_integer([:positive]))

    %{
      id: id,
      name: "domain #{id}",
      external_id: "external id #{id}",
      updated_at: DateTime.utc_now()
    }
    |> Map.merge(params)
  end
end
