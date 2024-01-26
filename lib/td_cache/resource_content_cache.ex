defmodule TdCache.ResourceContentCache do
  @moduledoc """
  Shared cache for Resource Content.
  """

  alias TdCache.Redix
  @i18n_resource_key :i18n_resource

  def put(
        %{
          locale: %{lang: lang},
          resource_type: resource_type,
          resource_id: resource_id,
          content: content
        },
        _opts \\ []
      ) do
    {content_keys, i18n_resource_keys} =
      Enum.reduce(content, {[], []}, fn {field, value}, {ckeys, rkeys} ->
        content_key = i18n_content_key(resource_type, resource_id, lang, field)

        {ckeys ++ [content_key, value], [content_key | rkeys]}
      end)

    i18n_resource_key = i18n_resource_key(resource_type)

    delete_commands = delete_commands(resource_type, resource_id, lang)

    commands =
      delete_commands ++
        [
          ["MSET" | content_keys],
          ["SADD", i18n_resource_key | i18n_resource_keys]
        ]

    Redix.transaction_pipeline(commands)
  end

  def get(resource_type, resource_id, lang, field) do
    field_key = i18n_content_key(resource_type, resource_id, lang, field)

    case Redix.command(["GET", field_key]) do
      {:ok, nil} ->
        nil

      {:ok, field_value} ->
        field_value
    end
  end

  def get_resource_keys(resource_type) do
    resource_key = i18n_resource_key(resource_type)

    case Redix.command(["SMEMBERS", resource_key]) do
      {:ok, nil} ->
        nil

      {:ok, field_value} ->
        field_value
    end
  end

  def delete(resource_type) do
    delete_commands = delete_commands(resource_type, "*", "*")

    case delete_commands do
      [] -> {:ok, []}
      [_ | _] -> Redix.transaction_pipeline(delete_commands)
    end
  end

  def delete(resource_type, resource_id) do
    delete_commands = delete_commands(resource_type, resource_id, "*")

    case delete_commands do
      [] -> {:ok, []}
      [_ | _] -> Redix.transaction_pipeline(delete_commands)
    end
  end

  def delete(resource_type, resource_id, lang) do
    delete_commands = delete_commands(resource_type, resource_id, lang)

    case delete_commands do
      [] -> {:ok, []}
      [_ | _] -> Redix.transaction_pipeline(delete_commands)
    end
  end

  def delete_commands(resource_type, resource_id, lang) do
    i18n_resource_key = i18n_resource_key(resource_type)

    i18n_content_lang_key = i18n_content_lang_key(resource_type, resource_id, lang)

    case Redix.command(["KEYS", i18n_content_lang_key <> ":*"]) do
      {:ok, []} ->
        []

      {:ok, redis_key_fields} ->
        [["DEL" | redis_key_fields], ["SREM", i18n_resource_key | redis_key_fields]]
    end
  end

  defp i18n_resource_key(resource_type),
    do: "#{@i18n_resource_key}:#{resource_type}:keys"

  defp i18n_content_key(resource_type, resource_id, lang, field),
    do: "#{@i18n_resource_key}:#{resource_type}:#{resource_id}:#{lang}:#{field}"

  defp i18n_content_lang_key(resource_type, resource_id, lang),
    do: "#{@i18n_resource_key}:#{resource_type}:#{resource_id}:#{lang}"
end
