defmodule TdCache.PermissionsTest do
  use ExUnit.Case

  alias TdCache.ConceptCache
  alias TdCache.DomainCache
  alias TdCache.IngestCache
  alias TdCache.Permissions
  alias TdCache.Redix
  alias TdCache.TaxonomyCache
  alias TdCache.UserCache

  doctest TdCache.Permissions

  setup do
    user = %{id: unique_id()}
    parent = %{id: unique_id(), name: "parent", updated_at: DateTime.utc_now()}

    domain = %{
      id: unique_id(),
      name: "child",
      parent_ids: [parent.id],
      updated_at: DateTime.utc_now()
    }

    {:ok, _} = TaxonomyCache.put_domain(domain)
    {:ok, _} = TaxonomyCache.put_domain(parent)

    concept = concept_entry(domain)
    {:ok, _} = ConceptCache.put(concept)

    ingest = ingest_entry(domain)
    {:ok, _} = IngestCache.put(ingest)

    acl_entries = acl_entries(domain, user, [["create_business_concept"], ["create_ingest"]])

    on_exit(fn ->
      ConceptCache.delete(concept.id)
      IngestCache.delete(ingest.id)

      Redix.del!([
        "session:*",
        "business_concept:ids:inactive",
        "business_concept:events",
        "domain:*",
        "domains:*",
        "permission:foo:roles",
        "permission:bar:roles"
      ])
    end)

    {:ok,
     concept: concept, domain: domain, ingest: ingest, parent: parent, acl_entries: acl_entries}
  end

  test "resolves cached session permissions", %{
    concept: concept,
    domain: domain,
    ingest: ingest,
    parent: parent,
    acl_entries: acl_entries
  } do
    import Permissions, only: :functions
    session_id = "#{unique_id()}"
    expire_at = DateTime.utc_now() |> DateTime.add(100) |> DateTime.to_unix()
    cache_session_permissions!(session_id, expire_at, acl_entries)
    assert has_permission?(session_id, :create_business_concept, "domain", domain.id)
    assert has_permission?(session_id, :create_business_concept, "domain", [domain.id, parent.id])
    assert has_permission?(session_id, :create_business_concept, "business_concept", concept.id)
    assert has_permission?(session_id, :create_ingest, "ingest", ingest.id)
    assert has_any_permission?(session_id, [:create_business_concept], "domain", domain.id)

    assert has_any_permission?(
             session_id,
             [:create_business_concept],
             "business_concept",
             concept.id
           )

    assert has_permission?(session_id, :create_business_concept)
    refute has_permission?(session_id, :manage_quality_rule)
  end

  describe "put_permission_roles/1 and get_permission_roles/1" do
    test "writes and reads roles by permission" do
      roles_by_permission = %{
        "foo" => ["role1", "role2"],
        "bar" => ["role4", "role3"]
      }

      assert {:ok, [0, 2, 0, 2]} = Permissions.put_permission_roles(roles_by_permission)
      assert {:ok, roles} = Permissions.get_permission_roles("foo")
      assert Enum.sort(roles) == ["role1", "role2"]
      assert {:ok, roles} = Permissions.get_permission_roles("bar")
      assert Enum.sort(roles) == ["role3", "role4"]
    end
  end

  describe "permitted_domain_ids/2" do
    setup do
      user = %{id: user_id = unique_id()}
      ts = DateTime.utc_now()

      parent = %{id: unique_id(), name: "parent", updated_at: ts}
      domain = %{id: unique_id(), parent_ids: [parent.id], name: "domain", updated_at: ts}

      child = %{
        id: unique_id(),
        parent_ids: [domain.id, parent.id],
        name: "child",
        updated_at: ts
      }

      {:ok, _} = Permissions.put_permission_roles(%{"foo" => ["role1", "role2"]})
      {:ok, _} = UserCache.put_roles(user_id, %{"role1" => [parent.id]})
      {:ok, _} = DomainCache.put(parent)
      {:ok, _} = DomainCache.put(domain)
      {:ok, _} = DomainCache.put(child)

      [user: user, parent: parent, domain: domain, child: child]
    end

    test "returns all permitted domain_ids, including descendents", %{
      user: user,
      domain: domain,
      parent: parent,
      child: child
    } do
      domain_ids = Permissions.permitted_domain_ids(user.id, "foo")
      assert Enum.sort(domain_ids) == Enum.sort([domain.id, parent.id, child.id])
    end
  end

  defp concept_entry(%{id: domain_id}) do
    id = unique_id()
    %{id: id, domain_id: domain_id, name: "concept #{id}", business_concept_version_id: id}
  end

  defp ingest_entry(%{id: domain_id}) do
    id = unique_id()
    %{id: id, domain_id: domain_id, name: "ingest #{id}", ingest_version_id: id}
  end

  defp acl_entry(%{id: domain_id}, %{id: user_id}, permissions) do
    %{
      resource_type: "domain",
      resource_id: domain_id,
      principal_type: "user",
      principal_id: user_id,
      permissions: permissions
    }
  end

  defp acl_entries(domain, user, permissions) do
    permissions
    |> Enum.map(&acl_entry(domain, user, &1))
  end

  defp unique_id, do: System.unique_integer([:positive])
end
