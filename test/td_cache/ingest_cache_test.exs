defmodule TdCache.IngestCacheTest do
  @moduledoc """
  Unit tests for ingest cache
  """

  use ExUnit.Case

  import TdCache.Factory

  alias TdCache.CacheHelpers
  alias TdCache.IngestCache
  alias TdCache.Redix

  doctest TdCache.IngestCache

  setup do
    %{id: domain_id} = domain = build(:domain)
    CacheHelpers.put_domain(domain)
    ingest = build(:ingest, domain_id: domain_id)
    on_exit(fn -> IngestCache.delete(ingest.id) end)

    [ingest: ingest]
  end

  test "put/1 returns Ok", %{ingest: ingest} do
    assert {:ok, [3, 1]} = IngestCache.put(ingest)
  end

  test "get_domain_id/1 from an ingest", %{ingest: ingest} do
    {:ok, _} = IngestCache.put(ingest)
    assert IngestCache.get_domain_id(ingest.id) == "#{ingest.domain_id}"
  end

  test "get_domain_ids/1 returns the domain id", %{ingest: ingest} do
    %{domain_id: domain_id, id: id} = ingest
    {:ok, _} = IngestCache.put(ingest)
    assert IngestCache.get_domain_ids(id) == [domain_id]
  end

  test "get_domain_ids/1 returns nil if ingest is missing" do
    refute IngestCache.get_domain_ids(nil)
  end

  test "get_name/1 from a ingest", %{ingest: ingest} do
    {:ok, _} = IngestCache.put(ingest)
    assert IngestCache.get_name(ingest.id) == ingest.name
  end

  test "get_ingest_version_id/1 from a ingest", %{ingest: ingest} do
    {:ok, _} = IngestCache.put(ingest)
    assert IngestCache.get_ingest_version_id(ingest.id) == "#{ingest.ingest_version_id}"
  end

  test "delete/1 deletes the ingest from cache", %{ingest: ingest} do
    {:ok, _} = IngestCache.put(ingest)
    assert {:ok, [1, 1]} = IngestCache.delete(ingest.id)
    refute Redix.exists?("ingest:#{ingest.id}")
  end
end
