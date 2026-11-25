defmodule TdCache.RedixTest do
  use ExUnit.Case

  alias TdCache.Redix

  @key "foofoofoo"

  setup do
    on_exit(fn -> Redix.del!(@key) end)
  end

  describe "connection_opts/1" do
    test "leaves out :password when it is nil" do
      opts = Redix.connection_opts(host: "redis", port: 6379, password: nil, name: :redix)

      assert opts == [host: "redis", port: 6379, name: :redix]
    end

    test "keeps :password when it is set" do
      opts = Redix.connection_opts(host: "redis", port: 6379, password: "secret")

      assert opts == [host: "redis", port: 6379, password: "secret"]
    end

    test "returns options that Redix accepts when no password is configured" do
      opts =
        Redix.connection_opts(
          host: Application.fetch_env!(:td_cache, :redis_host),
          port: Application.fetch_env!(:td_cache, :port),
          password: nil
        )

      assert {:ok, pid} = Elixir.Redix.start_link(opts)
      assert {:ok, "PONG"} = Elixir.Redix.command(pid, ["PING"])
      Elixir.Redix.stop(pid)
    end
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
        # no spaces, please
        Redix.to_integer_list!("1, 2, 3")
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
