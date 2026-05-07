defmodule TdCache.SessionCache do
  @moduledoc """
  Provides functionality for caching and checking existence of active sessions
  in Truedat's distributed cache.
  """

  alias TdCache.Redix

  @spec put(map()) :: :ok | {:error, any()}
  def put(%{"jti" => jti, "exp" => exp}) do
    put(jti, exp)
  end

  @spec put(binary, integer) :: :ok | {:error, any()}
  def put(jti, expire_at) when is_binary(jti) and is_integer(expire_at) do
    key = key(jti)

    case Redix.command(["SET", key, "", "NX", "EXAT", expire_at]) do
      {:ok, "OK"} -> :ok
      {:ok, nil} -> {:error, :exists}
      {:error, error} -> {:error, error}
    end
  end

  @spec exists?(binary) :: boolean
  def exists?(jti) when is_binary(jti) do
    jti
    |> key()
    |> Redix.exists?()
  end

  @spec delete(binary) :: :ok | {:error, any()}
  def delete(jti) when is_binary(jti) do
    keys = [key(jti) | Redix.keys!(key(jti) <> ":*")]

    case Redix.command(["DEL" | keys]) do
      {:ok, 0} -> {:error, :not_found}
      {:ok, _one_or_more} -> :ok
      {:error, e} -> {:error, e}
    end
  end

  @spec key(binary) :: binary
  defp key(jti), do: "session:" <> jti
end
