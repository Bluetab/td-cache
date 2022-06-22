defmodule TdCache.Templates.FieldFormatter do
  @moduledoc """
  Module for format template fields
  """
  alias TdCache.Permissions

  def format(%{"name" => "_confidential"} = field, ctx) do
    field
    |> Map.put("type", "string")
    |> Map.put("widget", "checkbox")
    |> Map.put("cardinality", "?")
    |> Map.put("default", "No")
    |> Map.put("disabled", is_confidential_field_disabled?(ctx))
  end

  def format(%{"type" => "user", "values" => %{"role_users" => role_name}} = field, ctx) do
    claims = Map.get(ctx, :claims, nil)
    user_roles = Map.get(ctx, :user_roles, %{})
    apply_role_meta(field, claims, role_name, user_roles)
  end

  def format(%{"type" => "user_group", "values" => %{"role_groups" => role_name}} = field, ctx) do
    claims = Map.get(ctx, :claims, nil)
    user_roles = Map.get(ctx, :user_roles, %{})
    user_group_roles = Map.get(ctx, :user_group_roles, %{})

    field
    |> apply_role_meta(claims, role_name, user_roles)
    |> apply_user_group_meta(role_name, user_group_roles)
  end

  def format(%{} = field, _ctx), do: field

  defp is_confidential_field_disabled?(%{claims: nil}), do: true

  defp is_confidential_field_disabled?(%{claims: %{role: "admin"}}), do: false

  defp is_confidential_field_disabled?(%{domain_id: domain_id, claims: %{jti: jti}}) do
    !Permissions.has_permission?(jti, :manage_confidential_business_concepts, "domain", domain_id)
  end

  defp is_confidential_field_disabled?(_), do: true

  defp apply_role_meta(
         %{"values" => values} = field,
         %{user_id: user_id} = _claims,
         role_name,
         user_roles
       )
       when not is_nil(role_name) do
    users = Map.get(user_roles, role_name, [])
    usernames = Enum.map(users, & &1.full_name)
    values = Map.put(values, "processed_users", usernames)
    field = Map.put(field, "values", values)

    case Enum.find(users, &(&1.id == user_id)) do
      nil -> field
      u -> Map.put(field, "default", u.full_name)
    end
  end

  defp apply_role_meta(field, _claims, _role, _user_roles), do: field

  defp apply_user_group_meta(
         %{"values" => values} = field,
         role_name,
         user_group_roles
       )
       when not is_nil(role_name) do
    groups = Map.get(user_group_roles, role_name, [])
    names = Enum.map(groups, & &1.name)
    values = Map.put(values, "processed_groups", names)
    Map.put(field, "values", values)
  end

  defp apply_user_group_meta(field, _role, _user_roles), do: field
end
