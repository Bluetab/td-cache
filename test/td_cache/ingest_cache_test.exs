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
      IngestCache.delete_ingest(ingest.id)
    end)

    {:ok, ingest: ingest}
  end

  test "put_ingest returns Ok", %{ingest: ingest} do
    assert IngestCache.put_ingest(ingest) == {:ok, "OK"}
  end

  test "get_parent_id from an ingest", %{ingest: ingest} do
    IngestCache.put_ingest(ingest)

    assert String.to_integer(IngestCache.get_parent_id(ingest.id)) ==
             ingest.domain_id
  end

  test "get_name from a ingest", %{ingest: ingest} do
    IngestCache.put_ingest(ingest)
    assert IngestCache.get_name(ingest.id) == ingest.name
  end

  test "get_ingest_version_id from a ingest", %{ingest: ingest} do
    IngestCache.put_ingest(ingest)

    assert String.to_integer(IngestCache.get_ingest_version_id(ingest.id)) ==
             ingest.ingest_version_id
  end

  test "delete_ingest deletes the ingest from cache", %{ingest: ingest} do
    IngestCache.put_ingest(ingest)
    IngestCache.delete_ingest(ingest.id)
    assert {:ok, 0} = Redix.command(["EXISTS", "ingest:#{ingest.id}"])
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
