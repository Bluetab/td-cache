defmodule TdCache.IngestCache do
  @moduledoc """
  Shared cache for Ingests.
  """
  alias TdCache.Redix

  def get_parent_id(ingest_id) do
    {:ok, parent_id} = Redix.command(["HGET", "ingest:#{ingest_id}", "parent_id"])
    parent_id
  end

  def get_name(ingest_id) do
    {:ok, name} = Redix.command(["HGET", "ingest:#{ingest_id}", "name"])
    name
  end

  def get_ingest_version_id(ingest_id) do
    {:ok, ingest_version_id} = Redix.command(["HGET", "ingest:#{ingest_id}", "ingest_version_id"])
    ingest_version_id
  end

  def put_ingest(%{
        id: ingest_id,
        domain_id: parent_id,
        name: name,
        ingest_version_id: ingest_version_id
      }) do
    Redix.command([
      "HMSET",
      "ingest:#{ingest_id}",
      "parent_id",
      parent_id,
      "name",
      name,
      "ingest_version_id",
      ingest_version_id
    ])
  end

  def delete_ingest(ingest_id) do
    Redix.command(["DEL", "ingest:#{ingest_id}"])
  end
end
