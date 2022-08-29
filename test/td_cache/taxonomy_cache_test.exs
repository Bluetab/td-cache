defmodule TdCache.TaxonomyCacheTest do
  use ExUnit.Case

  import TdCache.Factory

  alias TdCache.CacheHelpers
  alias TdCache.Redix
  alias TdCache.TaxonomyCache

  @role "test_role"

  doctest TdCache.TaxonomyCache

  setup do
    root = build(:domain)
    parent = build(:domain, parent_id: root.id)
    domain = build(:domain, parent_id: parent.id)

    on_exit(fn -> Redix.del!(["domain:*", "domains:*"]) end)

    [root: root, parent: parent, domain: domain]
  end

  test "put_domain returns OK", %{domain: domain} do
    assert {:ok, [0, 4, 1, 1, 1, 1, 0]} = TaxonomyCache.put_domain(domain)
  end

  test "put_domain forces refresh if specified", %{domain: domain} do
    assert {:ok, [0, 4, 1, 1, 1, 1, 0]} = TaxonomyCache.put_domain(domain)
    assert {:ok, []} = TaxonomyCache.put_domain(domain)
    assert {:ok, [1, 4, 0, 0, 0, 0, 0]} = TaxonomyCache.put_domain(domain, force: true)
  end

  test "put_domain invalidates local cache", %{domain: domain} do
    ConCache.put(:taxonomy, :tree, :foo)
    assert ConCache.get(:taxonomy, :tree)
    TaxonomyCache.put_domain(domain)
    refute ConCache.get(:taxonomy, :tree)
  end

  test "reaching_domain_ids returns parent ids including domain_id", %{
    domain: domain,
    parent: parent,
    root: root
  } do
    Enum.each([root, parent, domain], &CacheHelpers.put_domain/1)
    ids = TaxonomyCache.reaching_domain_ids(domain.id)
    assert ids == [domain.id, parent.id, root.id]
  end

  test "reaching_domain_ids accepts a list of ids", %{domain: domain, parent: parent, root: root} do
    Enum.each([root, parent, domain], &CacheHelpers.put_domain/1)
    ids = TaxonomyCache.reaching_domain_ids([domain.id])
    assert ids == [domain.id, parent.id, root.id]
  end

  test "reaching_domain_ids returns empty list if ids is empty", %{} do
    assert TaxonomyCache.reaching_domain_ids([]) == []
  end

  test "reachable_domain_ids returns empty list if ids is empty", %{} do
    assert TaxonomyCache.reachable_domain_ids([]) == []
  end

  test "domain_count returns count of cached domains", %{
    domain: domain,
    parent: parent,
    root: root
  } do
    Enum.each([root, parent, domain], &CacheHelpers.put_domain/1)
    assert TaxonomyCache.domain_count() == 3
  end

  test "get_parent_ids when domain has no parent a list with only the domain's id", %{
    domain: domain
  } do
    domain = Map.put(domain, :parent_id, nil)
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.reaching_domain_ids(domain.id) == [domain.id]
  end

  test "delete_domain deletes the domain from cache", %{domain: domain} do
    TaxonomyCache.put_domain(domain)
    TaxonomyCache.delete_domain(domain.id)
    refute Redix.exists?("domain:#{domain.id}")
  end

  test "delete_domain invalidates local cache" do
    ConCache.put(:taxonomy, :tree, :foo)
    assert ConCache.get(:taxonomy, :tree)
    TaxonomyCache.delete_domain(123)
    refute ConCache.get(:taxonomy, :tree)
  end

  test "get_by_external_id returns the domain if it exists",
       %{root: root, parent: parent, domain: domain} do
    domains = [root, parent, domain]

    for %{external_id: external_id} <- domains do
      refute TaxonomyCache.get_by_external_id(external_id)
    end

    Enum.map(domains, &TaxonomyCache.put_domain(&1))

    for %{id: id, external_id: external_id} <- domains do
      assert %{id: ^id} = TaxonomyCache.get_by_external_id(external_id)
    end
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
    Redix.del!("domain:deleted_ids")

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

  describe "has_role?/4" do
    setup %{
      root: root,
      parent: %{id: id2} = parent,
      domain: domain
    } do
      %{id: user_id} = user = build(:user)
      Enum.each([root, parent, domain], &TaxonomyCache.put_domain/1)
      CacheHelpers.put_user(user)
      CacheHelpers.put_acl("domain", id2, @role, [user_id])
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
end
