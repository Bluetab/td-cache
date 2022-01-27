defmodule TdCache.PermissionsTest do
  use ExUnit.Case

  import TdCache.Factory

  alias TdCache.CacheHelpers
  alias TdCache.Permissions
  alias TdCache.Redix
  alias TdCache.UserCache

  doctest TdCache.Permissions

  setup do
    user = build(:user)
    parent = build(:domain)
    domain = build(:domain, parent_ids: [parent.id])

    CacheHelpers.put_domain(parent)
    CacheHelpers.put_domain(domain)

    concept = build(:concept, domain_id: domain.id)
    CacheHelpers.put_concept(concept)

    ingest = build(:ingest, domain_id: domain.id)
    CacheHelpers.put_ingest(ingest)

    acl_entries = acl_entries(domain, user, [["create_business_concept"], ["create_ingest"]])

    on_exit(fn ->
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

    [concept: concept, domain: domain, ingest: ingest, parent: parent, acl_entries: acl_entries]
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

      assert {:ok, [2, 2]} = Permissions.put_permission_roles(roles_by_permission)
      assert {:ok, roles} = Permissions.get_permission_roles("foo")
      assert Enum.sort(roles) == ["role1", "role2"]
      assert {:ok, roles} = Permissions.get_permission_roles("bar")
      assert Enum.sort(roles) == ["role3", "role4"]
    end

    test "removes existing roles from permissions" do
      roles_by_permission = %{"foo" => ["role1", "role2"]}

      assert {:ok, [2]} = Permissions.put_permission_roles(roles_by_permission)
      assert {:ok, roles} = Permissions.get_permission_roles("foo")
      assert Enum.sort(roles) == ["role1", "role2"]

      roles_by_permission = %{"foo" => ["role1"]}

      assert {:ok, [1, 1]} = Permissions.put_permission_roles(roles_by_permission)
      assert {:ok, roles} = Permissions.get_permission_roles("foo")
      assert Enum.sort(roles) == ["role1"]
    end
  end

  describe "permitted_domain_ids/2" do
    setup do
      user = build(:user)

      parent = build(:domain)
      domain = build(:domain, parent_ids: [parent.id])
      child = build(:domain, parent_ids: [domain.id, parent.id])

      CacheHelpers.put_domain(parent)
      CacheHelpers.put_domain(domain)
      CacheHelpers.put_domain(child)

      {:ok, _} = Permissions.put_permission_roles(%{"foo" => ["role1", "role2"]})
      {:ok, _} = UserCache.put_roles(user.id, %{"role1" => [parent.id]})

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

    test "returns an empty list if no such key exists" do
      assert Permissions.permitted_domain_ids(123, "foo") == []
    end
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
    Enum.map(permissions, &acl_entry(domain, user, &1))
  end

  defp unique_id, do: System.unique_integer([:positive])
end
