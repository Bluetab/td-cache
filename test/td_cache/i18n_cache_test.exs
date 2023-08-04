defmodule TdCache.I18nCacheTest do
  use ExUnit.Case

  import TdCache.TestOperators

  alias TdCache.I18nCache

  @locales [:en]

  doctest TdCache.I18nCache

  setup do
    [messages_en] = Enum.map(@locales, &get_messages(&1))

    on_exit(fn ->
      Enum.map(@locales, &I18nCache.delete(&1))
    end)

    [messages_en: {:en, messages_en}]
  end

  test "put/2 returns Ok", %{messages_en: {locale, messages_en}} do
    Enum.each(messages_en, fn message ->
      assert {:ok, ["OK", 1]} = I18nCache.put(locale, message)
    end)
  end

  test "put/2 returns updates only when updated at is changed", %{
    messages_en: {locale, messages_en}
  } do
    put_messages(locale, messages_en)

    {message_id, definition} = get_ramdom_message(messages_en)

    new_definition = definition <> "_updated"

    assert {:ok, _} = I18nCache.put(locale, {message_id, new_definition})

    assert {:ok, ^new_definition} = I18nCache.get_definition(locale, message_id)
  end

  test "put/2 returns ok adding new message", %{messages_en: {locale, messages_en}} do
    put_messages(locale, messages_en)

    assert {:ok, ["OK", 1]} = I18nCache.put(locale, {"dx.dax", "dax_en"})

    assert length(I18nCache.list_by_locale(locale)) == 6
  end

  test "delete/1 return ok when all keys and messages was deleted by locale", %{
    messages_en: {locale, messages_en}
  } do
    put_messages(locale, messages_en)

    assert {:ok, [5, 1]} = I18nCache.delete(locale)

    messages = I18nCache.list_by_locale(locale)
    assert messages == []
  end

  test "delete/2 return ok when delete specific message_id", %{
    messages_en: {locale, messages_en}
  } do
    put_messages(locale, messages_en)
    {message_id, _definition} = get_ramdom_message(messages_en)

    assert {:ok, 1} = I18nCache.delete(locale, message_id)

    assert {:ok, nil} = I18nCache.get_definition(locale, message_id)
  end

  test "get_definition/2 return definition from message_id and locale", %{
    messages_en: {locale, messages_en}
  } do
    put_messages(locale, messages_en)
    {message_id, definition} = get_ramdom_message(messages_en)

    assert {:ok, ^definition} = I18nCache.get_definition(locale, message_id)
  end

  test "get_definition/2 return definition from message_key and locale", %{
    messages_en: {locale, messages_en}
  } do
    put_messages(locale, messages_en)
    {message_id, definition} = get_ramdom_message(messages_en)

    assert {:ok, ^definition} = I18nCache.get_definition(locale, "i18n:#{locale}:#{message_id}")
  end

  test "list_by_locale/1 return the list of definitions by locale", %{
    messages_en: {locale, messages_en}
  } do
    put_messages(locale, messages_en)

    messages = Enum.map(messages_en, fn {_, message} -> message end)

    assert messages <|> I18nCache.list_by_locale(locale)
  end

  test "map_keys_by_prefix/2 ", %{messages_en: {locale, messages_en}} do
    messages = %{
      "kr.kar" => "tar#{locale}",
      "kr.kar.taaar" => "taaar_#{locale}",
      "kr.kar.teeer" => "teeer_#{locale}",
      "kr.kar.tiiir" => "tiiir_#{locale}"
    }

    messages
    |> Map.merge(messages_en)
    |> (&put_messages(locale, &1)).()

    prefix = "kr.kar"

    assert messages <~> I18nCache.map_keys_by_prefix(locale, prefix)
  end

  defp put_messages(locale, messages) do
    Enum.each(messages, &I18nCache.put(locale, &1))
  end

  defp get_messages(locale) do
    %{
      "br.bar" => "bar_#{locale}",
      "fo.foo" => "foo_#{locale}",
      "bz.baz" => "faz_#{locale}",
      "yz.yxz" => "yxz_#{locale}",
      "qx.qux" => "qux_#{locale}"
    }
  end

  defp get_ramdom_message(messages) do
    Enum.at(messages, Enum.random(0..(Enum.count(messages) - 1)))
  end
end
