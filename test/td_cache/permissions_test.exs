defmodule TdCache.PermissionsTest do
  use ExUnit.Case
  alias TdCache.ConceptCache
  alias TdCache.IngestCache
  alias TdCache.Permissions
  alias TdCache.Redix
  alias TdCache.TaxonomyCache
  doctest TdCache.Permissions

  setup do
    user = %{id: random_id()}
    parent = %{id: random_id(), name: "parent"}
    domain = %{id: random_id(), name: "child", parent_ids: [parent.id]}
    {:ok, _} = TaxonomyCache.put_domain(domain)
    {:ok, _} = TaxonomyCache.put_domain(parent)

    concept = concept_entry(domain)
    {:ok, _} = ConceptCache.put(concept)

    ingest = ingest_entry(domain)
    {:ok, _} = IngestCache.put_ingest(ingest)

    acl_entries = acl_entries(domain, user, [["create_business_concept"], ["create_ingest"]])

    on_exit(fn ->
      TaxonomyCache.delete_domain(domain.id)
      TaxonomyCache.delete_domain(parent.id)
      ConceptCache.delete(concept.id)
      IngestCache.delete_ingest(ingest.id)
      Redix.del!("session:*")
    end)

    {:ok, concept: concept, domain: domain, ingest: ingest, acl_entries: acl_entries}
  end

  test "resolves cached session permissions", %{
    concept: concept,
    domain: domain,
    ingest: ingest,
    acl_entries: acl_entries
  } do
    import Permissions, only: :functions
    session_id = "#{random_id()}"
    now = DateTime.utc_now() |> DateTime.to_unix()
    cache_session_permissions!(session_id, now + 100, acl_entries)
    assert has_permission?(session_id, :create_business_concept, "domain", domain.id)
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

  defp concept_entry(%{id: domain_id}) do
    id = random_id()
    %{id: id, domain_id: domain_id, name: "concept #{id}", business_concept_version_id: id}
  end

  defp ingest_entry(%{id: domain_id}) do
    id = random_id()
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

  defp random_id, do: :rand.uniform(100_000_000)
end
