defmodule TdCache.I18nCache do
  @moduledoc """
  Shared cache for i18n.
  """
  use GenServer

  alias TdCache.Redix
  @i18n_key :i18n

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def put(locale, message, opts \\ []) do
    GenServer.call(__MODULE__, {:put, locale, message, opts})
  end

  def delete(locale) do
    GenServer.call(__MODULE__, {:delete, locale})
  end

  def delete(locale, message_id) do
    GenServer.call(__MODULE__, {:delete, locale, message_id})
  end

  def get_definition(locale, key, opts \\ [])

  def get_definition(_locale, "i18n:" <> _ = definition_key, opts) do
    response =
      case get_cache(definition_key, fn -> read_definition(definition_key) end, opts) do
        nil ->
          nil

        message ->
          message
      end

    {:ok, response}
  end

  def get_definition(locale, message_id, opts) do
    get_definition(locale, i18n_definition_key(locale, message_id), opts)
  end

  def list_by_locale(locale) do
    key = i18n_locale_key(locale)

    case(Redix.command(["SMEMBERS", key])) do
      {:ok, ids} ->
        Enum.map(ids, &read_definition(&1))

      _ ->
        []
    end
  end

  def map_keys_by_prefix(locale, prefix) do
    pattern = i18n_definition_key(locale, prefix <> "*")

    {:ok, keys} = Redix.command(["KEYS", pattern])

    keys
    |> Enum.into(%{}, fn definition_key ->
      {defition_key_to_message_id(locale, definition_key), read_definition(definition_key)}
    end)
  end

  ## Callbacks

  @impl true
  def init(_args) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:put, locale, message, opts}, _from, state) do
    reply = put_message(locale, message, opts)

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, locale}, _from, state) do
    reply = delete_locale(locale)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, locale, message_id}, _from, state) do
    reply = delete_message(locale, message_id)
    {:reply, reply, state}
  end

  ## Private Functions
  defp get_cache(key, fun, opts) do
    if Keyword.get(opts, :refresh, false) do
      message = fun.()
      ## REVIEW: TD-5891 que pasa cuando es nil el mensaje
      ConCache.put(@i18n_key, key, message)
      message
    else
      ConCache.get_or_store(@i18n_key, key, fn -> fun.() end)
    end
  end

  defp delete_cache(key) do
    ConCache.delete(@i18n_key, key)
  end

  defp read_definition(definition_key) do
    case Redix.command(["GET", definition_key]) do
      {:ok, nil} ->
        nil

      {:ok, message} ->
        message
    end
  end

  defp delete_locale(locale) do
    locale_key = i18n_locale_key(locale)

    keys_to_delete =
      case(Redix.command(["SMEMBERS", locale_key])) do
        {:ok, ids} -> ids
        _ -> []
      end

    response =
      keys_to_delete
      |> Enum.chunk_every(1000)
      |> Enum.map(&["DEL" | &1])
      |> Kernel.++([["DEL", locale_key]])
      |> Redix.transaction_pipeline()

    Enum.each(keys_to_delete, &delete_cache(&1))
    response
  end

  defp delete_message(locale, message_id) do
    definition_key = i18n_definition_key(locale, message_id)
    commands = ["DEL", definition_key]

    response = Redix.command(commands)
    :ok = delete_cache(definition_key)
    response
  end

  defp put_message(locale, %{message_id: message_id, definition: definition} = _message, _opts) do
    locale_key = i18n_locale_key(locale)
    definition_key = i18n_definition_key(locale, message_id)

    commands = [
      ["SET", definition_key, definition],
      ["SADD", locale_key, definition_key]
    ]

    response = Redix.transaction_pipeline(commands)
    :ok = delete_cache(definition_key)
    response
  end

  defp i18n_locale_key(locale), do: "#{@i18n_key}:keys:#{locale}"

  defp i18n_definition_key(locale, message_id), do: "#{@i18n_key}:#{locale}:#{message_id}"

  defp defition_key_to_message_id(locale, definition_key) when is_atom(locale),
    do: defition_key_to_message_id(Atom.to_string(locale), definition_key)

  defp defition_key_to_message_id(locale, "i18n:" <> locale_message_id) do
    [locale_message, message_id] = String.split(locale_message_id, ":")

    if locale == locale_message, do: message_id, else: nil
  end
end
