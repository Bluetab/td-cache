defmodule TdCache.NonceCacheTest do
  use ExUnit.Case
  alias TdCache.NonceCache
  doctest TdCache.NonceCache

  test "a nonce exists in the cache after creation" do
    nonce = NonceCache.create_nonce()
    assert NonceCache.exists?(nonce)
    NonceCache.pop(nonce)
  end

  test "a nonce value can be read once after creation" do
    nonce = NonceCache.create_nonce("Some value")
    assert NonceCache.exists?(nonce)
    assert NonceCache.pop(nonce) == "Some value"
    assert NonceCache.pop(nonce) == nil
  end
end
