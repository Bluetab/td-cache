defmodule TdCache.Factory do
  @moduledoc """
  An `ExMachina` factory for `TdCache` tests.
  """

  use ExMachina

  def domain_factory do
    %{
      id: unique_id(),
      name: sequence("domain_name"),
      external_id: sequence("domain_external_id"),
      parent_id: nil,
      updated_at: DateTime.utc_now()
    }
  end

  def concept_factory do
    %{
      id: unique_id(),
      type: sequence("concept_type"),
      business_concept_version_id: unique_id(),
      name: sequence("concept_name"),
      content: build(:content)
    }
  end

  def ingest_factory do
    %{
      id: unique_id(),
      ingest_version_id: unique_id(),
      name: sequence("ingest_name")
    }
  end

  def content_factory do
    %{
      "data_owner" => sequence("data_owner"),
      "foo" => ["bar", "baz"]
    }
  end

  def user_factory do
    %{
      id: unique_id(),
      full_name: sequence("full_name"),
      user_name: sequence("user_name"),
      email: sequence("user_email"),
      external_id: sequence("user_external_id"),
    }
  end

  defp unique_id, do: System.unique_integer([:positive])
end
