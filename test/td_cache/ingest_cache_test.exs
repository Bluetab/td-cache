defmodule TdCache.IngestCacheTest do
  @moduledoc """
  Unit tests for ingest cache
  """

  use ExUnit.Case

  alias TdCache.IngestCache
  alias TdCache.Redix

  doctest TdCache.IngestCache

  setup do
    ingest = ingest_fixture()

    on_exit(fn ->
      IngestCache.delete(ingest.id)
    end)

    {:ok, ingest: ingest}
  end

  test "put returns Ok", %{ingest: ingest} do
    assert {:ok, ["OK", 1]} = IngestCache.put(ingest)
  end

  test "get_domain_id from an ingest", %{ingest: ingest} do
    {:ok, _} = IngestCache.put(ingest)

    assert IngestCache.get_domain_id(ingest.id) == "#{ingest.domain_id}"
  end

  test "get_domain_ids from an ingest", %{ingest: ingest} do
    %{domain_id: domain_id, id: id} = ingest
    {:ok, _} = IngestCache.put(ingest)

    assert IngestCache.get_domain_ids(id) == [domain_id]
  end

  test "get_name from a ingest", %{ingest: ingest} do
    {:ok, _} = IngestCache.put(ingest)

    assert IngestCache.get_name(ingest.id) == ingest.name
  end

  test "get_ingest_version_id from a ingest", %{ingest: ingest} do
    {:ok, _} = IngestCache.put(ingest)

    assert IngestCache.get_ingest_version_id(ingest.id) == "#{ingest.ingest_version_id}"
  end

  test "delete deletes the ingest from cache", %{ingest: ingest} do
    {:ok, _} = IngestCache.put(ingest)
    assert {:ok, [1, 1]} = IngestCache.delete(ingest.id)
    assert not Redix.exists?("ingest:#{ingest.id}")
  end

  defp ingest_fixture do
    id = :rand.uniform(100_000_000)

    %{
      id: id,
      domain_id: :rand.uniform(100_000_000),
      name: "ingest #{id}",
      ingest_version_id: :rand.uniform(100_000_000)
    }
  end
end
