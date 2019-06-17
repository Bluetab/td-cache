defmodule TdCache.SystemCacheTest do
  use ExUnit.Case
  alias TdCache.SystemCache
  doctest TdCache.SystemCache

  setup do
    system = %{id: :rand.uniform(100_000_000), external_id: "foo", name: "bar"}

    on_exit(fn ->
      SystemCache.delete(system.id)
    end)

    {:ok, system: system}
  end

  describe "SystemCache" do
    test "starts automatically" do
      assert Process.whereis(SystemCache)
    end

    test "writes a system entry in redis and reads it back", context do
      system = context[:system]
      {:ok, _} = SystemCache.put(system)
      {:ok, s} = SystemCache.get(system.id)
      assert s == system
    end

    test "deletes an entry in redis", context do
      system = context[:system]
      {:ok, _} = SystemCache.put(system)
      {:ok, _} = SystemCache.delete(system.id)
      assert {:ok, nil} == SystemCache.get(system.id)
    end
  end
end
