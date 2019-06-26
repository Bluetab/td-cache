defmodule TdCache.TaxonomyCacheTest do
  use ExUnit.Case
  alias TdCache.Redix
  alias TdCache.TaxonomyCache
  doctest TdCache.TaxonomyCache

  setup do
    root = random_domain()
    parent = random_domain() |> Map.put(:parent_ids, [root.id])
    domain = random_domain() |> Map.put(:parent_ids, [parent.id, root.id])

    on_exit(fn ->
      Redix.del!("domain:*")
      Redix.del!("domains:*")
    end)

    {:ok, root: root, parent: parent, domain: domain}
  end

  test "put_domain returns OK", context do
    domain = context[:domain]
    assert {:ok, ["OK", 1, 1, 0]} = TaxonomyCache.put_domain(domain)
  end

  test "get_parent_ids with self returns parent ids including domain_id", context do
    domain = context[:domain]
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_parent_ids(domain.id) == [domain.id | domain.parent_ids]
  end

  test "get_parent_ids without self returns parent ids excluding domain_id", context do
    domain = context[:domain]
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_parent_ids(domain.id, false) == domain.parent_ids
  end

  test "get_parent_ids when domain has no parents returns an empty list", context do
    domain =
      context[:domain]
      |> Map.put(:parent_ids, [])

    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_parent_ids(domain.id, false) == []
  end

  test "get_name returns name", context do
    domain = context[:domain]
    TaxonomyCache.put_domain(domain)
    assert TaxonomyCache.get_name(domain.id) == domain.name
  end

  test "delete_domain deletes the domain from cache", context do
    domain = context[:domain]
    TaxonomyCache.put_domain(domain)
    TaxonomyCache.delete_domain(domain.id)
    assert not Redix.exists?("domain:#{domain.id}")
  end

  test "get_domain_name_to_id_map returns a map with names as keys and ids as values", context do
    domains =
      [:root, :parent, :domain]
      |> Enum.map(&Map.get(context, &1))

    domains
    |> Enum.map(&TaxonomyCache.put_domain(&1))

    map = TaxonomyCache.get_domain_name_to_id_map()

    domains
    |> Enum.all?(&Map.has_key?(map, &1.name))
    |> assert
  end

  defp random_domain do
    id = random_id()
    %{id: id, name: "domain #{id}"}
  end

  defp random_id, do: :rand.uniform(100_000_000)
end
