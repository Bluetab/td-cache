defmodule TdCache.Templates.AclLoaderTest do
  use ExUnit.Case

  import TdCache.TestOperators

  alias TdCache.AclCache
  alias TdCache.CacheHelpers
  alias TdCache.Templates.AclLoader

  setup do
    on_exit(fn -> TdCache.Redix.del!("acl_*") end)
  end

  describe "get_roles_and_users/1" do
    test "returns list of users in role" do
      domain1 = CacheHelpers.insert_domain()
      domain2 = CacheHelpers.insert_domain()

      %{id: user_id_1, full_name: user_name_1} = CacheHelpers.insert_user()
      %{id: user_id_2, full_name: user_name_2} = CacheHelpers.insert_user()
      %{id: user_id_3} = CacheHelpers.insert_user()

      AclCache.set_acl_roles("domain", domain1.id, ["role1", "role2"])
      AclCache.set_acl_roles("domain", domain2.id, ["role2", "role3"])

      AclCache.set_acl_role_users("domain", domain1.id, "role1", [user_id_1])
      AclCache.set_acl_role_users("domain", domain1.id, "role2", [user_id_2])
      AclCache.set_acl_role_users("domain", domain2.id, "role2", [user_id_1, user_id_2])
      AclCache.set_acl_role_users("domain", domain2.id, "role3", [user_id_3])

      assert %{
               "role1" => [%{id: ^user_id_1}],
               "role2" => [%{id: ^user_id_2}]
             } = AclLoader.get_roles_and_users([domain1.id])

      assert %{
               "role1" => [%{id: ^user_id_1}],
               "role2" => role2,
               "role3" => [%{id: ^user_id_3}]
             } = AclLoader.get_roles_and_users([domain1.id, domain2.id])

      assert role2 |||
               [
                 %{id: user_id_1, full_name: user_name_1},
                 %{id: user_id_2, full_name: user_name_2}
               ]
    end
  end

  describe "get_roles_and_groups/1" do
    test "returns list of groups in role" do
      domain1 = CacheHelpers.insert_domain()
      domain2 = CacheHelpers.insert_domain()

      %{id: group_id_1} = CacheHelpers.insert_group()
      %{id: group_id_2} = CacheHelpers.insert_group()
      %{id: group_id_3} = CacheHelpers.insert_group()

      AclCache.set_acl_group_roles("domain", domain1.id, ["role1", "role2"])
      AclCache.set_acl_group_roles("domain", domain2.id, ["role2", "role3"])

      AclCache.set_acl_role_groups("domain", domain1.id, "role1", [group_id_1])
      AclCache.set_acl_role_groups("domain", domain1.id, "role2", [group_id_2])
      AclCache.set_acl_role_groups("domain", domain2.id, "role2", [group_id_1, group_id_2])
      AclCache.set_acl_role_groups("domain", domain2.id, "role3", [group_id_3])

      AclCache.get_acl_role_groups("domain", domain1.id, "role1")

      assert %{
               "role1" => [%{id: ^group_id_1}],
               "role2" => [%{id: ^group_id_2}]
             } = AclLoader.get_roles_and_groups([domain1.id])

      assert %{
               "role1" => [%{id: ^group_id_1}],
               "role2" => [%{id: id1}, %{id: id2}],
               "role3" => [%{id: ^group_id_3}]
             } = AclLoader.get_roles_and_groups([domain1.id, domain2.id])

      assert Enum.sort([id1, id2]) == Enum.sort([group_id_1, group_id_2])
    end
  end
end
