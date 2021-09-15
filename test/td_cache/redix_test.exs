defmodule TdCache.RedixTest do
  use ExUnit.Case

  alias TdCache.Redix

  @key "foofoofoo"

  setup do
    on_exit(fn -> Redix.del!(@key) end)
  end

  describe "to_integer_list!" do
    test "returns an empty list for nil or empty string" do
      assert Redix.to_integer_list!(nil) == []
      assert Redix.to_integer_list!("") == []
    end

    test "converts a comma-separated string to a list of integers" do
      assert Redix.to_integer_list!("1,2,3") == [1, 2, 3]
    end

    test "converts a custom eparated string to a list of integers" do
      assert Redix.to_integer_list!("1 π 2 π 3", " π ") == [1, 2, 3]
    end

    test "raises if values are not integers" do
      assert_raise ArgumentError, fn ->
        Redix.to_integer_list!("xyz")
      end

      assert_raise ArgumentError, fn ->
        Redix.to_integer_list!("1, 2, 3") # no spaces, please
      end
    end
  end

  describe "read_map/2" do
    test "reads a hash from Redis and returns a map" do
      Redix.command(["HSET", @key, "k1", "v1", "k2", "v2 ", "k3", "v3"])

      assert {:ok, %{"k1" => 2, "k2" => 3, "k3" => 2}} =
               Redix.read_map(@key, fn [k, v] -> {k, String.length(v)} end)
    end
  end
end
