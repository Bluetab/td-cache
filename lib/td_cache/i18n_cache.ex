defmodule TdCache.I18nCache do
  @moduledoc """
  Shared cache for i18n.
  """
  use GenServer

  alias TdCache.Redix
  @i18n_key :i18n

  @default_lang Application.compile_env(:td_cache, :lang, "en")

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def put(lang, message, opts \\ []) do
    GenServer.call(__MODULE__, {:put, lang, message, opts})
  end

  def delete(lang) do
    GenServer.call(__MODULE__, {:delete, lang})
  end

  def delete(lang, message_id) do
    GenServer.call(__MODULE__, {:delete, lang, message_id})
  end

  def get_definition(lang, key, opts \\ [])

  def get_definition(_lang, "i18n:" <> _ = definition_key, opts) do
    case get_cache(definition_key, fn -> read_definition(definition_key) end, opts) do
      nil ->
        nil

      message ->
        message
    end
  end

  def get_definition(lang, message_id, opts) do
    case get_definition(lang, i18n_definition_key(lang, message_id), opts) do
      nil ->
        Keyword.get(opts, :default_value)

      definition ->
        definition
    end
  end

  def get_definitions_by_value(value, lang, opts \\ [])

  def get_definitions_by_value(value, lang, opts) do
    prefix = Keyword.get(opts, :prefix, "")

    lang
    |> map_keys_by_prefix(prefix)
    |> Enum.filter(fn {_, v} -> v == value end)
    |> Enum.map(fn {message_id, definition} ->
      %{definition: definition, message_id: message_id}
    end)
  end

  def list_by_lang(lang) do
    key = i18n_lang_key(lang)

    case(Redix.command(["SMEMBERS", key])) do
      {:ok, ids} ->
        Enum.map(ids, &read_definition(&1))

      _ ->
        []
    end
  end

  def map_keys_by_prefix(lang, prefix) do
    pattern = i18n_definition_key(lang, prefix <> "*")

    {:ok, keys} = Redix.command(["KEYS", pattern])

    Enum.into(keys, %{}, fn definition_key ->
      {defition_key_to_message_id(lang, definition_key), read_definition(definition_key)}
    end)
  end

  def put_default_locale(default_locale) do
    Redix.command(["SET", "i18n:locales:default", default_locale])
  end

  def put_required_locales([]) do
    {:ok, _} = Redix.command(["DEL", "i18n:locales:required"])
    {:ok, [0, 0]}
  end

  def put_required_locales(required_locales) do
    Redix.transaction_pipeline([
      ["DEL", "i18n:locales:required"],
      ["RPUSH", "i18n:locales:required", required_locales]
    ])
  end

  def get_default_locale do
    case Redix.command(["GET", "i18n:locales:default"]) do
      {:ok, nil} -> {:ok, @default_lang}
      response -> response
    end
  end

  def get_required_locales do
    Redix.read_list("i18n:locales:required")
  end

  ## Callbacks

  @impl true
  def init(_args) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:put, lang, message, opts}, _from, state) do
    reply = put_message(lang, message, opts)

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, lang}, _from, state) do
    reply = delete_lang(lang)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, lang, message_id}, _from, state) do
    reply = delete_message(lang, message_id)
    {:reply, reply, state}
  end

  ## Private Functions
  defp get_cache(key, fun, opts) do
    if Keyword.get(opts, :refresh, false) do
      message = fun.()
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

  defp delete_lang(lang) do
    lang_key = i18n_lang_key(lang)

    keys_to_delete =
      case(Redix.command(["SMEMBERS", lang_key])) do
        {:ok, ids} -> ids
        _ -> []
      end

    response =
      keys_to_delete
      |> Enum.chunk_every(1000)
      |> Enum.map(&["DEL" | &1])
      |> Kernel.++([["DEL", lang_key]])
      |> Redix.transaction_pipeline()

    Enum.each(keys_to_delete, &delete_cache(&1))
    response
  end

  defp delete_message(lang, message_id) do
    definition_key = i18n_definition_key(lang, message_id)
    commands = ["DEL", definition_key]

    response = Redix.command(commands)
    :ok = delete_cache(definition_key)
    response
  end

  defp put_message(lang, %{message_id: message_id, definition: definition} = _message, _opts) do
    lang_key = i18n_lang_key(lang)
    definition_key = i18n_definition_key(lang, message_id)

    commands = [
      ["SET", definition_key, definition],
      ["SADD", lang_key, definition_key]
    ]

    response = Redix.transaction_pipeline(commands)
    :ok = delete_cache(definition_key)
    response
  end

  defp i18n_lang_key(lang), do: "#{@i18n_key}:keys:#{lang}"

  defp i18n_definition_key(lang, message_id), do: "#{@i18n_key}:#{lang}:#{message_id}"

  defp defition_key_to_message_id(lang, definition_key) when is_atom(lang),
    do: defition_key_to_message_id(Atom.to_string(lang), definition_key)

  defp defition_key_to_message_id(lang, "i18n:" <> lang_message_id) do
    [lang_message, message_id] = String.split(lang_message_id, ":")

    if lang == lang_message, do: message_id, else: nil
  end
end
