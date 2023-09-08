defmodule TdCache.I18nCacheTest do
  use ExUnit.Case

  import TdCache.TestOperators

  alias TdCache.I18nCache

  @langs ["en"]

  doctest TdCache.I18nCache

  setup do
    %{en: messages_en, es: messages_es} = get_messages()

    on_exit(fn ->
      Enum.map(@langs, &I18nCache.delete(&1))
    end)

    [messages_en: {:en, messages_en}, messages_es: {:es, messages_es}]
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

    assert {:ok, [9, 1]} = I18nCache.delete(lang)

    messages = I18nCache.list_by_lang(lang)
    assert messages == []
  end

  test "delete/2 return ok when delete specific message_id", %{
    messages_en: {lang, messages_en}
  } do
    put_messages(lang, messages_en)
    %{message_id: message_id} = get_ramdom_message(messages_en)

    assert {:ok, 1} = I18nCache.delete(lang, message_id)

    assert is_nil(I18nCache.get_definition(lang, message_id))
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

  test "get_definitions_by_value/2 return definitions from lang and value",
       %{
         messages_en: {lang_en, messages_en},
         messages_es: {lang_es, messages_es}
       } do
    put_messages(lang_en, messages_en)
    put_messages(lang_es, messages_es)

    value = "val2_es"

    assert [
             %{
               message_id: "br.bar.val2",
               definition: ^value
             },
             %{
               message_id: "fo.foo.val2",
               definition: ^value
             }
           ] = I18nCache.get_definitions_by_value(value, lang_es)
  end

  test "get_definitions_by_value/3 return definitions from lang value and prefix provided",
       %{
         messages_en: {lang_en, messages_en},
         messages_es: {lang_es, messages_es}
       } do
    put_messages(lang_en, messages_en)
    put_messages(lang_es, messages_es)

    value = "val2_es"
    prefix = "br.bar"

    assert [
             %{
               message_id: "br.bar.val2",
               definition: ^value
             }
           ] = I18nCache.get_definitions_by_value(value, lang_es, prefix: prefix)
  end

  test "translate_definitions_by_value/4 translate definitions from lang value to lang_to provided",
       %{
         messages_en: {lang_en, messages_en},
         messages_es: {lang_es, messages_es}
       } do
    put_messages(lang_en, messages_en)
    put_messages(lang_es, messages_es)

    lang_from = "es"
    lang_to = "en"

    value = "val2_es"

    assert [
             %{
               message_id: "br.bar.val2",
               definition_to: "val2_en",
               definition_from: "val2_es"
             },
             %{
               message_id: "fo.foo.val2",
               definition_to: "val2_en",
               definition_from: "val2_es"
             }
           ] = I18nCache.translate_definitions_by_value(value, lang_from, lang_to)
  end

  test "translate_definitions_by_value/4 translate definitions from lang value to lang_to provided with prefix",
       %{
         messages_en: {lang_en, messages_en},
         messages_es: {lang_es, messages_es}
       } do
    put_messages(lang_en, messages_en)
    put_messages(lang_es, messages_es)

    lang_from = "es"
    lang_to = "en"

    value = "val2_es"
    prefix = "br.bar"

    assert [
             %{
               message_id: "br.bar.val2",
               definition_to: "val2_en",
               definition_from: "val2_es"
             }
           ] = I18nCache.translate_definitions_by_value(value, lang_from, lang_to, prefix: prefix)
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

  defp get_messages() do
    %{
      en: [
        %{message_id: "br.bar.val1", definition: "val1_en"},
        %{message_id: "br.bar.val2", definition: "val2_en"},
        %{message_id: "br.bar.val3", definition: "val3_en"},
        %{message_id: "fo.foo.val1", definition: "val1_en"},
        %{message_id: "fo.foo.val2", definition: "val2_en"},
        %{message_id: "fo.foo.val3", definition: "val3_en"},
        %{message_id: "bz.baz", definition: "faz_en"},
        %{message_id: "yz.yxz", definition: "yxz_en"},
        %{message_id: "qx.qux", definition: "qux_en"}
      ],
      es: [
        %{message_id: "br.bar.val1", definition: "val1_es"},
        %{message_id: "br.bar.val2", definition: "val2_es"},
        %{message_id: "br.bar.val3", definition: "val3_es"},
        %{message_id: "fo.foo.val1", definition: "val1_es"},
        %{message_id: "fo.foo.val2", definition: "val2_es"},
        %{message_id: "fo.foo.val3", definition: "val3_es"},
        %{message_id: "bz.baz", definition: "faz_es"},
        %{message_id: "yz.yxz", definition: "yxz_es"},
        %{message_id: "qx.qux", definition: "qux_es"}
      ]
    }
  end

  defp get_ramdom_message(messages) do
    Enum.at(messages, Enum.random(0..(Enum.count(messages) - 1)))
  end
end
