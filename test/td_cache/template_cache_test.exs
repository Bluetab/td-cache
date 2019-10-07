defmodule TdCache.TemplateCacheTest do
  use ExUnit.Case
  alias TdCache.TemplateCache
  doctest TdCache.TemplateCache

  setup do
    templates = 0..10 |> Enum.map(fn _ -> random_template() end) |> Enum.uniq() |> Enum.take(4)

    on_exit(fn ->
      templates
      |> Enum.map(& &1.id)
      |> Enum.each(&TemplateCache.delete/1)

      ConCache.delete(:templates, :all)
    end)

    {:ok, templates: templates}
  end

  test "put/1 returns Ok", context do
    [template | _] = context[:templates]
    assert {:ok, ["OK", 1, 1]} == TemplateCache.put(template)
  end

  test "put/1 returns updates only when updated at is changed", context do
    [template | _] = context[:templates]
    assert {:ok, ["OK", 1, 1]} == TemplateCache.put(template)
    assert {:ok, []} == TemplateCache.put(template)
  end

  test "get/1 gets content", context do
    [template | _] = context[:templates]
    {:ok, _} = TemplateCache.put(template)
    {:ok, content} = TemplateCache.get(template.id, :content)
    assert content == template.content
  end

  test "get_by_name gets template", context do
    [template | _] = context[:templates]
    TemplateCache.put(template)
    {:ok, t} = TemplateCache.get_by_name(template.name)
    assert t.content == template.content
    assert t.id == template.id
    assert t.name == template.name
    assert t.scope == template.scope
    assert t.updated_at == to_string(template.updated_at)
  end

  test "get_by_name invalid key will return nil" do
    assert {:ok, nil} == TemplateCache.get_by_name("invalid:key")
  end

  test "list will return a list of objects", context do
    context[:templates]
    |> Enum.take(3)
    |> Enum.map(&TemplateCache.put/1)

    ConCache.delete(:templates, :all)
    {:ok, list} = TemplateCache.list()
    assert length(list) == 3
  end

  test "list_by_scope will only return template from the requested scope", context do
    templates =
      context[:templates]
      |> Enum.take(6)
      |> Enum.chunk_every(3)
      |> Enum.with_index()
      |> Enum.flat_map(fn {templates, i} ->
        Enum.map(templates, &Map.put(&1, :scope, "scope_#{i}"))
      end)

    templates
    |> Enum.map(&TemplateCache.put/1)

    {:ok, list} = TemplateCache.list_by_scope("scope_0")
    assert length(list) == 3
  end

  test "delete/1 deletes from cache", context do
    [template | _] = context[:templates]
    TemplateCache.put(template)
    assert {:ok, [1, 1, 1]} == TemplateCache.delete(template.id)
    assert {:ok, nil} == TemplateCache.get(template.id)
  end

  defp random_template do
    id = random_id()

    %{
      id: id,
      name: "Template #{id}",
      label: "Label #{id}",
      scope: "Scope #{id}",
      content: [%{"name" => "field", "type" => "string"}],
      updated_at: DateTime.utc_now()
    }
  end

  defp random_id, do: :rand.uniform(100_000_000)
end
