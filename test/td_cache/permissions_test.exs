defmodule TdCache.PermissionsTest do
  use ExUnit.Case

  import Assertions
  import TdCache.Factory

  alias TdCache.CacheHelpers
  alias TdCache.Permissions
  alias TdCache.Redix

  doctest TdCache.Permissions

  setup do
    parent = build(:domain)
    domain = build(:domain, parent_ids: [parent.id])

    CacheHelpers.put_domain(parent)
    CacheHelpers.put_domain(domain)

    concept = build(:concept, domain_id: domain.id)
    CacheHelpers.put_concept(concept)

    ingest = build(:ingest, domain_id: domain.id)
    CacheHelpers.put_ingest(ingest)

    implementation = build(:implementation, domain_id: domain.id)
    CacheHelpers.put_implementation(implementation)

    permissions = %{
      "create_business_concept" => [domain.id],
      "create_ingest" => [domain.id],
      "manage_quality_rule_implementations" => [domain.id]
    }

    on_exit(fn ->
      Redix.del!([
        "session:*",
        "business_concept:ids:inactive",
        "business_concept:events",
        "domain:*",
        "domains:*",
        "permission:foo:roles",
        "permission:bar:roles",
        "permission:defaults"
      ])
    end)

    [
      concept: concept,
      domain: domain,
      implementation: implementation,
      ingest: ingest,
      parent: parent,
      permissions: permissions
    ]
  end

  test "considers default permissions", %{} do
    refute Permissions.has_permission?("any_session_id", :foo, "domain", "123")
    refute Permissions.has_permission?("any_session_id", :foo)
    Permissions.put_default_permissions(["foo"])
    assert Permissions.has_permission?("any_session_id", :foo, "domain", "123")
    assert Permissions.has_permission?("any_session_id", :foo, "any_type", "any_id")
    assert Permissions.has_permission?("any_session_id", :foo)
  end

  test "resolves cached session permissions", %{
    concept: concept,
    domain: domain,
    implementation: implementation,
    ingest: ingest,
    parent: parent,
    permissions: permissions
  } do
    import Permissions, only: :functions
    session_id = "#{unique_id()}"
    expire_at = DateTime.utc_now() |> DateTime.add(100) |> DateTime.to_unix()
    cache_session_permissions!(session_id, expire_at, permissions)
    assert has_permission?(session_id, :create_business_concept, "domain", domain.id)
    assert has_permission?(session_id, :create_business_concept, "domain", [domain.id, parent.id])
    assert has_permission?(session_id, :create_business_concept, "business_concept", concept.id)

    assert has_permission?(
             session_id,
             :manage_quality_rule_implementations,
             "implementation",
             implementation.id
           )

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
      parent = build(:domain)
      domain = build(:domain, parent_id: parent.id)
      child = build(:domain, parent_id: domain.id)

      CacheHelpers.put_domain(parent)
      CacheHelpers.put_domain(domain)
      CacheHelpers.put_domain(child)

      session_id = "#{unique_id()}"
      expire_at = DateTime.utc_now() |> DateTime.add(100) |> DateTime.to_unix()
      Permissions.cache_session_permissions!(session_id, expire_at, %{"foo" => [parent.id]})

      [session_id: session_id, parent: parent, domain: domain, child: child]
    end

    test "returns all permitted domain_ids, including descendents", %{
      session_id: session_id,
      domain: domain,
      parent: parent,
      child: child
    } do
      domain_ids = Permissions.permitted_domain_ids(session_id, "foo")
      assert_lists_equal(domain_ids, [parent.id, domain.id, child.id])
    end

    test "returns an empty list if no such key exists" do
      assert Permissions.permitted_domain_ids("invalid_session", "foo") == []
    end
  end

  describe "put_default_permissions/1" do
    setup do
      on_exit(fn -> Redix.command!(["DEL", "permission:defaults"]) end)
    end

    test "replaces the set of default permissions" do
      assert Redix.command!(["SCARD", "permission:defaults"]) == 0
      assert {:ok, [_, 2]} = Permissions.put_default_permissions(["foo", "bar", "foo"])
      assert Redix.command!(["SCARD", "permission:defaults"]) == 2
      assert {:ok, 1} = Permissions.put_default_permissions([])
      assert Redix.command!(["SCARD", "permission:defaults"]) == 0
    end
  end

  describe "is_default_permission?/1" do
    test "returns true iff the permission exists in the default permissions" do
      Permissions.put_default_permissions([])
      refute Permissions.is_default_permission?("foo")
      Permissions.put_default_permissions(["foo", "bar"])
      assert Permissions.is_default_permission?("foo")
      assert Permissions.is_default_permission?(:bar)
      refute Permissions.is_default_permission?("baz")
    end
  end

  defp unique_id, do: System.unique_integer([:positive])
end
