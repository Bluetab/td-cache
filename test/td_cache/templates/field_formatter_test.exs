defmodule TdCache.Templates.FieldFormatterTest do
  use ExUnit.Case

  alias TdCache.Templates.FieldFormatter

  describe "template preprocessor" do
    test "format/2 formats the _confidential field" do
      field = %{"name" => "_confidential", "foo" => "bar"}
      ctx = %{}

      expected = %{
        "default" => "No",
        "disabled" => true,
        "foo" => "bar",
        "name" => "_confidential",
        "cardinality" => "?",
        "type" => "string",
        "widget" => "checkbox"
      }

      assert FieldFormatter.format(field, ctx) == expected
    end

    test "format/2 applies role metadata" do
      field = %{"name" => "foo", "type" => "user", "values" => %{"role_users" => "owner"}}
      users = [%{id: 1, full_name: "User 1"}, %{id: 2, full_name: "User 2"}]
      user_roles = %{"owner" => users}
      claims = %{user_id: 2}
      ctx = %{user_roles: user_roles, claims: claims}

      expected = %{
        "default" => "User 2",
        "name" => "foo",
        "type" => "user",
        "values" => %{
          "role_users" => "owner",
          "processed_users" => ["User 1", "User 2"]
        }
      }

      assert FieldFormatter.format(field, ctx) == expected
    end

    test "format/2 does not affect fields with no metadata" do
      field = %{"foo" => "bar"}
      ctx = %{}

      assert FieldFormatter.format(field, ctx) == field
    end

    test "format/2 applies user group metadata with group details" do
      field = %{"name" => "foo", "type" => "user_group", "values" => %{"role_groups" => "owner"}}

      groups = [
        %{id: 1, name: "group_1", alias: "Group 1"},
        %{id: 2, name: "group_2", alias: nil}
      ]

      user_group_roles = %{"owner" => groups}
      ctx = %{user_group_roles: user_group_roles}

      expected = %{
        "name" => "foo",
        "type" => "user_group",
        "values" => %{
          "role_groups" => "owner",
          "processed_groups" => ["Group 1", "group_2"],
          "processed_groups_details" => [
            %{"id" => 1, "name" => "group_1", "alias" => "Group 1", "display_name" => "Group 1"},
            %{"id" => 2, "name" => "group_2", "alias" => nil, "display_name" => "group_2"}
          ]
        }
      }

      assert FieldFormatter.format(field, ctx) == expected
    end

    test "format/2 normalizes empty group alias to nil in processed_groups_details" do
      field = %{"name" => "foo", "type" => "user_group", "values" => %{"role_groups" => "owner"}}
      groups = [%{id: 1, name: "group_1", alias: ""}]
      user_group_roles = %{"owner" => groups}
      ctx = %{user_group_roles: user_group_roles}

      expected = %{
        "name" => "foo",
        "type" => "user_group",
        "values" => %{
          "role_groups" => "owner",
          "processed_groups" => ["group_1"],
          "processed_groups_details" => [
            %{"id" => 1, "name" => "group_1", "alias" => nil, "display_name" => "group_1"}
          ]
        }
      }

      assert FieldFormatter.format(field, ctx) == expected
    end
  end
end
