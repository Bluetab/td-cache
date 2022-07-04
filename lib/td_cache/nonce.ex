defmodule TdCache.Nonce do
  @moduledoc """
  Provides a way of generating random nonces.
  """

  def new(length) do
    length
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64()
    |> binary_part(0, length)
  end
end
