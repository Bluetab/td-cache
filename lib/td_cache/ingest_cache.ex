defmodule TdCache.IngestCache do
  @moduledoc """
  Shared cache for Ingests.
  """

  alias TdCache.Redix
  alias TdCache.TaxonomyCache

  @keys "ingest:keys"
  @props [:name, :ingest_version_id, :domain_id]

  def get_name(id) do
    Redix.command!(["HGET", "ingest:#{id}", "name"])
  end

  def get_domain_id(id) do
    Redix.command!(["HGET", "ingest:#{id}", "domain_id"])
  end

  def get_domain_ids(id) do
    case get_domain_id(id) do
      nil ->
        nil

      domain_id ->
        domain_id
        |> String.to_integer()
        |> TaxonomyCache.reaching_domain_ids()
    end
  end

  def get_ingest_version_id(id) do
    {:ok, ingest_version_id} = Redix.command(["HGET", "ingest:#{id}", "ingest_version_id"])
    ingest_version_id
  end

  def get(id) do
    case Redix.read_map("ingest:#{id}") do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, ingest} ->
        ingest = Map.put(ingest, :id, id)
        {:ok, ingest}
    end
  end

  def put(%{id: id} = ingest) do
    commands = [
      ["HSET", "ingest:#{id}", Map.take(ingest, @props)],
      ["SADD", @keys, "ingest:#{id}"]
    ]

    results = Redix.transaction_pipeline!(commands)
    {:ok, results}
  end

  def delete(id) do
    commands = [
      ["DEL", "ingest:#{id}"],
      ["SREM", @keys, "ingest:#{id}"]
    ]

    results = Redix.transaction_pipeline!(commands)
    {:ok, results}
  end
end
