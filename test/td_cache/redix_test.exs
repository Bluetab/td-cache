defmodule TdCache.RedixTest do
  use ExUnit.Case

  alias TdCache.Redix

  @key "foofoofoo"

  setup do
    on_exit(fn -> Redix.del!(@key) end)
  end

  describe "read_map/2" do
    test "reads a hash from Redis and returns a map" do
      Redix.command(["HSET", @key, "k1", "v1", "k2", "v2 ", "k3", "v3"])

      assert {:ok, %{"k1" => 2, "k2" => 3, "k3" => 2}} =
               Redix.read_map(@key, fn [k, v] -> {k, String.length(v)} end)
    end
  end
end
