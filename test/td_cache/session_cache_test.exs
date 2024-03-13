defmodule TdCache.SessionCacheTest do
  use ExUnit.Case

  alias TdCache.Nonce
  alias TdCache.Redix
  alias TdCache.SessionCache

  setup do
    now = DateTime.utc_now() |> DateTime.to_unix()
    jti = Nonce.new(16)
    on_exit(fn -> SessionCache.delete(jti) end)
    [jti: jti, exp: now + 10]
  end

  describe "SessionCache.put/2" do
    test "sets an expiring key in cache", %{jti: jti, exp: exp} do
      assert :ok = SessionCache.put(jti, exp)
      assert {:ok, n} = Redix.command(["TTL", "session:" <> jti])
      assert n <= 10
    end

    test "returns error if key exists", %{jti: jti, exp: exp} do
      assert :ok = SessionCache.put(jti, exp)
      assert {:error, :exists} = SessionCache.put(jti, exp)
    end
  end

  describe "SessionCache.exists?/1" do
    test "returns true if session exists in cache", %{jti: jti, exp: exp} do
      refute SessionCache.exists?(jti)
      assert :ok = SessionCache.put(jti, exp)
      assert SessionCache.exists?(jti)
    end
  end

  describe "SessionCache.delete/1" do
    test "returns ok if session was deleted", %{jti: jti, exp: exp} do
      assert {:error, :not_found} = SessionCache.delete(jti)
      assert :ok = SessionCache.put(jti, exp)
      assert :ok = SessionCache.put(jti <> ":domain:permissions", exp)
      assert :ok = SessionCache.put(jti <> ":structure:permissions", exp)
      assert :ok = SessionCache.delete(jti)
      assert [] = Redix.keys!("session:" <> jti <> "*")
    end
  end
end
