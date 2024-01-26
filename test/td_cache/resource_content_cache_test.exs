defmodule TdCache.ResourceContentCacheTest do
  use ExUnit.Case
  alias TdCache.ResourceContentCache

  setup do
    content = %{"field1" => "var1", "field2" => "var2", "field3" => "var3"}
    resource_type = "res_type"
    lang = "es"
    resource_id = 1000

    resource_content = %{
      locale: %{lang: lang},
      resource_type: resource_type,
      resource_id: resource_id,
      content: content
    }

    on_exit(fn ->
      nil
      ResourceContentCache.delete(resource_type)
    end)

    [
      resource_content: resource_content,
      content: content,
      resource_type: resource_type,
      lang: lang,
      resource_id: resource_id
    ]
  end

  describe "resourceContentCache" do
    test "put i18n_resource content", %{resource_content: resource_content} do
      assert {:ok, ["OK", 3]} = ResourceContentCache.put(resource_content)

      # assert {:ok, ["OK", 3]} =
      #          ResourceContentCache.put(generate_resource_content("my_type", 1001, "fr"))

      # assert {:ok, ["OK", 3]} =
      #          ResourceContentCache.put(generate_resource_content("res_type", 1000, "fr"))

      # assert {:ok, ["OK", 3]} =
      #          ResourceContentCache.put(generate_resource_content("res_type", 1001, "es"))

      # assert {:ok, ["OK", 3]} =
      #          ResourceContentCache.put(generate_resource_content("my_type", 1000, "es"))

      assert "var1" == ResourceContentCache.get("res_type", 1000, "es", "field1")
      assert "var2" == ResourceContentCache.get("res_type", 1000, "es", "field2")
      assert "var3" == ResourceContentCache.get("res_type", 1000, "es", "field3")

      resource_keys_list = ResourceContentCache.get_resource_keys("res_type")
      assert length(resource_keys_list) == 3

      assert resource_keys_list
             |> Enum.any?(fn value -> value == "i18n_resource:res_type:1000:es:field1" end)

      assert resource_keys_list
             |> Enum.any?(fn value -> value == "i18n_resource:res_type:1000:es:field2" end)

      assert resource_keys_list
             |> Enum.any?(fn value -> value == "i18n_resource:res_type:1000:es:field3" end)
    end

    test "get value on found i18n_resource content", %{resource_content: resource_content} do
      assert {:ok, ["OK", 3]} = ResourceContentCache.put(resource_content)
      assert "var1" == ResourceContentCache.get("res_type", 1000, "es", "field1")
    end

    test "get error on not found i18n_resource content", %{resource_content: resource_content} do
      assert {:ok, ["OK", 3]} = ResourceContentCache.put(resource_content)
      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field4")
    end

    test "update value for i18n_resource content", %{resource_content: resource_content} do
      assert {:ok, ["OK", 3]} = ResourceContentCache.put(resource_content)
      assert "var1" == ResourceContentCache.get("res_type", 1000, "es", "field1")
      assert "var2" == ResourceContentCache.get("res_type", 1000, "es", "field2")
      assert "var3" == ResourceContentCache.get("res_type", 1000, "es", "field3")

      updated_field_value =
        resource_content
        |> Map.get(:content)
        |> Map.drop(["field3"])
        |> Map.put("field4", "var4")
        |> Map.put("field1", "var5")

      update_resource_content = Map.put(resource_content, :content, updated_field_value)

      assert {:ok, [3, 3, "OK", 3]} = ResourceContentCache.put(update_resource_content)

      assert "var5" == ResourceContentCache.get("res_type", 1000, "es", "field1")
      assert "var2" == ResourceContentCache.get("res_type", 1000, "es", "field2")
      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field3")
      assert "var4" == ResourceContentCache.get("res_type", 1000, "es", "field4")

      resource_keys_list = ResourceContentCache.get_resource_keys("res_type")
      assert length(resource_keys_list) == 3

      assert resource_keys_list
             |> Enum.any?(fn value -> value == "i18n_resource:res_type:1000:es:field1" end)

      assert resource_keys_list
             |> Enum.any?(fn value -> value == "i18n_resource:res_type:1000:es:field2" end)

      assert resource_keys_list
             |> Enum.any?(fn value -> value == "i18n_resource:res_type:1000:es:field4" end)

      refute resource_keys_list
             |> Enum.any?(fn value -> value == "i18n_resource:res_type:1000:es:field3" end)
    end

    test "deletes resource type", %{
      resource_content: resource_content,
      resource_type: resource_type
    } do
      assert {:ok, ["OK", 3]} = ResourceContentCache.put(resource_content)

      assert {:ok, [3, 3]} = ResourceContentCache.delete(resource_type)

      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field1")
      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field2")
      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field3")

      assert [] = ResourceContentCache.get_resource_keys("res_type")
    end

    test "deletes resource", %{
      resource_content: resource_content,
      resource_type: resource_type,
      resource_id: resource_id
    } do
      assert {:ok, ["OK", 3]} = ResourceContentCache.put(resource_content)

      assert {:ok, [3, 3]} = ResourceContentCache.delete(resource_type, resource_id)

      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field1")
      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field2")
      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field3")

      assert [] = ResourceContentCache.get_resource_keys("res_type")
    end

    test "deletes resource lang", %{
      resource_content: resource_content,
      resource_type: resource_type,
      resource_id: resource_id,
      lang: lang
    } do
      assert {:ok, ["OK", 3]} = ResourceContentCache.put(resource_content)

      assert {:ok, [3, 3]} = ResourceContentCache.delete(resource_type, resource_id, lang)

      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field1")
      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field2")
      assert nil == ResourceContentCache.get("res_type", 1000, "es", "field3")

      assert [] = ResourceContentCache.get_resource_keys("res_type")
    end
  end

  defp generate_resource_content(resource_type, resource_id, lang) do
    resource_content = %{
      locale: %{lang: lang},
      resource_type: resource_type,
      resource_id: resource_id,
      content: %{"field1" => "var1", "field2" => "var2", "field3" => "var3"}
    }
  end
end
