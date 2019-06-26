defmodule TdCache.NonceCache do
  @moduledoc """
  Shared cache for nonces.
  """
  alias TdCache.Redix

  @doc """
  Create a nonce with a specified length and expiry.
  """
  def create_nonce(value \\ "", length \\ 16, expiry_seconds \\ 3600) do
    nonce = generate_random_string(length)
    key = create_key(nonce)
    "OK" = Redix.command!(["SETEX", key, expiry_seconds, value])
    nonce
  end

  @doc """
  Returns true if the given nonce exists, false otherwise.
  """
  def exists?(nonce) do
    key = create_key(nonce)
    Redix.command!(["EXISTS", key]) == 1
  end

  @doc """
  Pops a specified nonce.
  """
  def pop(nonce) do
    key = create_key(nonce)

    {:ok, [nonce, _]} =
      Redix.transaction_pipeline([
        ["GET", key],
        ["DEL", key]
      ])

    nonce
  end

  defp generate_random_string(length) do
    length
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64()
    |> binary_part(0, length)
  end

  defp create_key(nonce) do
    "nonce:#{nonce}"
  end
end
