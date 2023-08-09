defmodule TdCache.I18nCacheTest do
  use ExUnit.Case

  import TdCache.TestOperators

  alias TdCache.I18nCache

  @langs ["en"]

  doctest TdCache.I18nCache

  setup do
    [messages_en] = Enum.map(@langs, &get_messages(&1))

    on_exit(fn ->
      Enum.map(@langs, &I18nCache.delete(&1))
    end)

    [messages_en: {:en, messages_en}]
  end

  test "put/2 returns Ok", %{messages_en: {lang, messages_en}} do
    Enum.each(messages_en, fn message ->
      assert {:ok, ["OK", 1]} = I18nCache.put(lang, message)
    end)
  end

  test "put/2 returns updates only when updated at is changed", %{
    messages_en: {lang, messages_en}
  } do
    put_messages(lang, messages_en)

    %{message_id: message_id, definition: definition} = get_ramdom_message(messages_en)

    new_definition = definition <> "_updated"

    assert {:ok, _} = I18nCache.put(lang, %{message_id: message_id, definition: new_definition})

    assert ^new_definition = I18nCache.get_definition(lang, message_id)
  end

  test "put/2 returns ok adding new message", %{messages_en: {lang, messages_en}} do
    put_messages(lang, messages_en)

    old_size = length(I18nCache.list_by_lang(lang))
    assert {:ok, ["OK", 1]} = I18nCache.put(lang, %{message_id: "dx.dax", definition: "dax_en"})

    assert length(I18nCache.list_by_lang(lang)) == old_size + 1
  end

  test "delete/1 return ok when all keys and messages was deleted by lang", %{
    messages_en: {lang, messages_en}
  } do
    put_messages(lang, messages_en)

    assert {:ok, [5, 1]} = I18nCache.delete(lang)

    messages = I18nCache.list_by_lang(lang)
    assert messages == []
  end

  test "delete/2 return ok when delete specific message_id", %{
    messages_en: {lang, messages_en}
  } do
    put_messages(lang, messages_en)
    %{message_id: message_id} = get_ramdom_message(messages_en)

    assert {:ok, 1} = I18nCache.delete(lang, message_id)

    assert ^message_id = I18nCache.get_definition(lang, message_id)
    assert "boo" = I18nCache.get_definition(lang, message_id, default_value: "boo")
  end

  test "get_definition/2 return definition from message_id and lang", %{
    messages_en: {lang, messages_en}
  } do
    put_messages(lang, messages_en)
    %{message_id: message_id, definition: definition} = get_ramdom_message(messages_en)

    assert ^definition = I18nCache.get_definition(lang, message_id)
  end

  test "get_definition/2 return definition from message_key and lang", %{
    messages_en: {lang, messages_en}
  } do
    put_messages(lang, messages_en)
    %{message_id: message_id, definition: definition} = get_ramdom_message(messages_en)

    assert ^definition = I18nCache.get_definition(lang, "i18n:#{lang}:#{message_id}")
  end

  test "list_by_lang/1 return the list of definitions by lang", %{
    messages_en: {lang, messages_en}
  } do
    put_messages(lang, messages_en)

    messages = Enum.map(messages_en, fn %{definition: message} -> message end)

    assert messages <|> I18nCache.list_by_lang(lang)
  end

  test "map_keys_by_prefix/2 ", %{messages_en: {lang, _messages_en}} do
    messages = %{
      "kr.kar" => "kar_#{lang}",
      "kr.kar.taaar" => "taaar_#{lang}",
      "kr.kar.teeer" => "teeer_#{lang}",
      "kr.kar.tiiir" => "tiiir_#{lang}"
    }

    messages
    |> Enum.map(fn {message_id, definition} ->
      %{message_id: message_id, definition: definition}
    end)
    |> (&put_messages(lang, &1)).()

    prefix = "kr.kar"

    assert messages <~> I18nCache.map_keys_by_prefix(lang, prefix)
  end

  defp put_messages(lang, messages) do
    Enum.each(messages, &I18nCache.put(lang, &1))
  end

  defp get_messages(lang) do
    [
      %{message_id: "br.bar", definition: "bar_#{lang}"},
      %{message_id: "fo.foo", definition: "foo_#{lang}"},
      %{message_id: "bz.baz", definition: "faz_#{lang}"},
      %{message_id: "yz.yxz", definition: "yxz_#{lang}"},
      %{message_id: "qx.qux", definition: "qux_#{lang}"}
    ]
  end

  defp get_ramdom_message(messages) do
    Enum.at(messages, Enum.random(0..(Enum.count(messages) - 1)))
  end
end
