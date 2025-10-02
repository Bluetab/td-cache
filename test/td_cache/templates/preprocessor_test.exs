defmodule TdCache.Templates.PreprocessorTest do
  use ExUnit.Case

  alias TdCache.AclCache
  alias TdCache.CacheHelpers
  alias TdCache.Templates.Preprocessor

  @role_name "foo_role"

  describe "template preprocessor" do
    test "preprocess_template/2 with domain_id uses role data cache to format content" do
      %{id: domain_id} = CacheHelpers.insert_domain()
      %{id: user_id, full_name: full_name} = CacheHelpers.insert_user()

      AclCache.set_acl_roles("domain", domain_id, [@role_name])
      AclCache.set_acl_role_users("domain", domain_id, @role_name, [user_id])

      ctx = %{domain_ids: [domain_id], claims: %{user_id: user_id}}

      fields = [
        %{"name" => "_confidential", "foo" => "bar"},
        %{"name" => "user_field", "type" => "user", "values" => %{"role_users" => @role_name}},
        %{"foo" => "bar"}
      ]

      template = %{content: [%{"name" => "group1", "fields" => fields}]}

      actual = Preprocessor.preprocess_template(template, ctx)
      assert %{content: [%{"fields" => fields}]} = actual
      assert [confidential_field, users_field, unchanged_field] = fields

      assert confidential_field == %{
               "cardinality" => "?",
               "default" => "No",
               "disabled" => true,
               "foo" => "bar",
               "name" => "_confidential",
               "type" => "string",
               "widget" => "checkbox"
             }

      assert users_field == %{
               "default" => full_name,
               "name" => "user_field",
               "type" => "user",
               "values" => %{"processed_users" => [full_name], "role_users" => @role_name}
             }

      assert unchanged_field == %{"foo" => "bar"}
    end

    test "preprocess_template/2 enriches user_group role fields" do
      %{id: domain_id} = CacheHelpers.insert_domain()
      %{id: user_id, full_name: full_name} = CacheHelpers.insert_user()
      %{id: group_id, name: group_name} = CacheHelpers.insert_group()

      AclCache.set_acl_roles("domain", domain_id, [@role_name])
      AclCache.set_acl_group_roles("domain", domain_id, [@role_name])
      AclCache.set_acl_role_users("domain", domain_id, @role_name, [user_id])
      AclCache.set_acl_role_groups("domain", domain_id, @role_name, [group_id])

      ctx = %{domain_ids: [domain_id], claims: %{user_id: user_id}}

      fields = [
        %{
          "name" => "user_group_field",
          "type" => "user_group",
          "values" => %{"role_groups" => @role_name}
        },
        %{"foo" => "bar"}
      ]

      template = %{content: [%{"name" => "group1", "fields" => fields}]}

      actual = Preprocessor.preprocess_template(template, ctx)
      assert %{content: [%{"fields" => fields}]} = actual
      assert [user_groups_field, unchanged_field] = fields

      assert user_groups_field == %{
               "default" => full_name,
               "name" => "user_group_field",
               "type" => "user_group",
               "values" => %{
                 "role_groups" => @role_name,
                 "processed_users" => [full_name],
                 "processed_groups" => [group_name]
               }
             }

      assert unchanged_field == %{"foo" => "bar"}
    end

    test "preprocess_template/2 with multiple domain_ids uses role data cache to format content" do
      %{id: domain_id_1} = CacheHelpers.insert_domain()
      %{id: domain_id_2} = CacheHelpers.insert_domain()
      %{id: user_id_1, full_name: full_name_1} = CacheHelpers.insert_user()
      %{id: user_id_2, full_name: full_name_2} = CacheHelpers.insert_user()
      %{id: user_id_3, full_name: full_name_3} = CacheHelpers.insert_user()

      AclCache.set_acl_roles("domain", domain_id_1, [@role_name])
      AclCache.set_acl_roles("domain", domain_id_2, [@role_name])

      AclCache.set_acl_role_users("domain", domain_id_1, @role_name, [user_id_2, user_id_3])
      AclCache.set_acl_role_users("domain", domain_id_2, @role_name, [user_id_1, user_id_2])

      ctx = %{domain_ids: [domain_id_1, domain_id_2], claims: %{user_id: user_id_1}}

      fields = [
        %{"name" => "user_field", "type" => "user", "values" => %{"role_users" => @role_name}},
        %{"foo" => "bar"}
      ]

      template = %{content: [%{"name" => "group1", "fields" => fields}]}

      actual = Preprocessor.preprocess_template(template, ctx)
      assert %{content: [%{"fields" => fields}]} = actual
      assert [users_field, unchanged_field] = fields

      assert %{
               "default" => ^full_name_1,
               "name" => "user_field",
               "type" => "user",
               "values" => %{"processed_users" => processed_users, "role_users" => @role_name}
             } = users_field

      assert Enum.sort(processed_users) == Enum.sort([full_name_1, full_name_2, full_name_3])

      assert unchanged_field == %{"foo" => "bar"}
    end

    test "preprocess_template/2 process dynamic table type fields" do
      %{id: domain_id} = CacheHelpers.insert_domain()
      %{id: user_id, full_name: full_name} = CacheHelpers.insert_user()
      %{id: group_id, name: group_name} = CacheHelpers.insert_group()

      AclCache.set_acl_roles("domain", domain_id, [@role_name])
      AclCache.set_acl_group_roles("domain", domain_id, [@role_name])
      AclCache.set_acl_role_users("domain", domain_id, @role_name, [user_id])
      AclCache.set_acl_role_groups("domain", domain_id, @role_name, [group_id])

      ctx = %{domain_ids: [domain_id], claims: %{user_id: user_id}}

      fields = [
        %{"name" => "user_field", "type" => "user", "values" => %{"role_users" => @role_name}},
        %{"foo" => "bar"},
        %{
          "name" => "table_field",
          "type" => "dynamic_table",
          "values" => %{
            "table_columns" => [
              %{
                "name" => "user_col",
                "type" => "user",
                "values" => %{"role_users" => @role_name}
              },
              %{"name" => "_confidential", "foo" => "bar"},
              %{
                "name" => "user_group_field_col",
                "type" => "user_group",
                "values" => %{"role_groups" => @role_name}
              }
            ]
          }
        }
      ]

      template = %{content: [%{"name" => "group1", "fields" => fields}]}

      assert %{content: [%{"fields" => fields}]} = Preprocessor.preprocess_template(template, ctx)

      assert Enum.find(fields, &(&1["name"] == "user_field")) == %{
               "default" => full_name,
               "name" => "user_field",
               "type" => "user",
               "values" => %{
                 "processed_users" => [full_name],
                 "role_users" => @role_name
               }
             }

      assert %{"values" => %{"table_columns" => columns}} =
               Enum.find(fields, &(&1["name"] == "table_field"))

      assert Enum.find(columns, &(&1["name"] == "user_col")) == %{
               "default" => full_name,
               "name" => "user_col",
               "type" => "user",
               "values" => %{
                 "processed_users" => [full_name],
                 "role_users" => @role_name
               }
             }

      assert Enum.find(columns, &(&1["name"] == "_confidential")) == %{
               "cardinality" => "?",
               "default" => "No",
               "disabled" => true,
               "foo" => "bar",
               "name" => "_confidential",
               "type" => "string",
               "widget" => "checkbox"
             }

      assert Enum.find(columns, &(&1["name"] == "user_group_field_col")) == %{
               "default" => full_name,
               "name" => "user_group_field_col",
               "type" => "user_group",
               "values" => %{
                 "processed_groups" => [group_name],
                 "processed_users" => [full_name],
                 "role_groups" => "foo_role"
               }
             }
    end
  end
end
