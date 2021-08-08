defmodule TdCache.PermissionsConfig do
  @moduledoc """
  Static config of permissions
  """
  @permissions [
    :is_admin,
    :create_acl_entry,
    :update_acl_entry,
    :delete_acl_entry,
    :create_domain,
    :update_domain,
    :delete_domain,
    :view_domain,
    :create_business_concept,
    :create_data_structure,
    :update_business_concept,
    :update_data_structure,
    :send_business_concept_for_approval,
    :delete_business_concept,
    :delete_data_structure,
    :publish_business_concept,
    :reject_business_concept,
    :deprecate_business_concept,
    :manage_business_concept_alias,
    :view_data_structure,
    :view_draft_business_concepts,
    :view_approval_pending_business_concepts,
    :view_published_business_concepts,
    :view_versioned_business_concepts,
    :view_rejected_business_concepts,
    :view_deprecated_business_concepts,
    :manage_business_concept_links,
    :manage_quality_rule,
    :manage_confidential_business_concepts,
    :create_ingest,
    :update_ingest,
    :send_ingest_for_approval,
    :delete_ingest,
    :publish_ingest,
    :reject_ingest,
    :deprecate_ingest,
    :view_draft_ingests,
    :view_approval_pending_ingests,
    :view_published_ingests,
    :view_versioned_ingests,
    :view_rejected_ingests,
    :view_deprecated_ingests,
    :manage_confidential_structures,
    :manage_ingest_relations,
    :view_data_structures_profile,
    :view_quality_rule,
    :manage_quality_rule_implementations,
    :execute_quality_rule_implementations,
    :link_data_structure,
    :manage_structures_metadata,
    :view_dashboard,
    :view_lineage,
    :manage_raw_quality_rule_implementations,
    :manage_data_sources,
    :manage_configurations,
    :profile_structures,
    :link_data_structure_tag,
    :create_structure_note,
    :edit_structure_note,
    :send_structure_note_to_approval,
    :reject_structure_note,
    :unreject_structure_note,
    :deprecate_structure_note,
    :publish_structure_note,
    :delete_structure_note,
    :view_structure_note,
    :view_structure_note_history,
    :publish_structure_note_from_draft,
    :manage_structures_domain,
    :view_grants,
    :manage_grants,
    :share_with_domain,
    :create_grant_request
  ]

  def permissions, do: @permissions
end
