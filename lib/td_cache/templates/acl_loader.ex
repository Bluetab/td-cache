defmodule TdCache.Templates.AclLoader do
  @moduledoc """
  The Permissions context.
  """

  alias TdCache.AclCache
  alias TdCache.TaxonomyCache
  alias TdCache.UserCache

  def get_roles_and_users(domain_ids) do
    domain_ids
    |> Enum.map(&get_roles_and_users_by_domain/1)
    |> Enum.reduce(
      %{},
      &Map.merge(&1, &2, fn _k, users_domain, users_acc ->
        users_acc
        |> Enum.concat(users_domain)
        |> Enum.sort_by(fn %{id: id} -> id end)
        |> Enum.dedup_by(fn %{id: id} -> id end)
      end)
    )
  end

  defp get_roles_and_users_by_domain(domain_id) do
    domain_id
    |> TaxonomyCache.reaching_domain_ids()
    |> Enum.map(fn domain_id -> {domain_id, AclCache.get_acl_roles("domain", domain_id)} end)
    |> Enum.flat_map(fn {domain_id, roles} -> fetch_users_by_role(domain_id, roles) end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.into(%{}, fn {role, users} -> {role, flatten_user_list(users)} end)
  end

  def fetch_users_by_role(domain_id, roles) do
    Enum.map(roles, &{&1, get_users_by_domain_and_role(domain_id, &1)})
  end

  defp get_users_by_domain_and_role(domain_id, role) do
    "domain"
    |> AclCache.get_acl_role_users(domain_id, role)
    |> Enum.map(fn user_id ->
      case UserCache.get(user_id) do
        {:ok, nil} -> nil
        {:ok, user} -> Map.take(user, [:id, :full_name])
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp flatten_user_list(users) do
    users |> List.flatten() |> Enum.uniq_by(& &1.id)
  end
end
