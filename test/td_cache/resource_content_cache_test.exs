defmodule TdCache.ResourceContentCacheTest do
  use ExUnit.Case
  alias TdCache.ResourceContentCache

  setup do
    content = %{"field1" => "var1", "field2" => "var2", "field3" => "var3"}

    resource_content = %{
      locale: %{lang: "es"},
      resource_type: "res_type",
      resource_id: 1000,
      content: content
    }

    [resource_content: resource_content]
    ## Hacer el on exit con los argumentos especificos del borrado
    ## ResourceContentCache.delete(...)
  end

  describe "resourceContentCache" do
    test "write a i18n_resource content and reads it back", %{resource_content: resource_content} do
      IO.inspect(resource_content, label: "resource_content ->")

      ResourceContentCache.put(resource_content)
    end
  end
end
