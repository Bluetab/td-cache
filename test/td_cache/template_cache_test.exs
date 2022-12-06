defmodule TdCache.TemplateCacheTest do
  use ExUnit.Case

  alias TdCache.Redix
  alias TdCache.Redix.Stream
  alias TdCache.TemplateCache

  @name_to_id_key "templates:name_to_id"

  doctest TdCache.TemplateCache

  setup do
    templates = 0..10 |> Enum.map(fn _ -> random_template(true) end) |> Enum.uniq() |> Enum.take(4)

    on_exit(fn ->
      templates
      |> Enum.map(& &1.id)
      |> Enum.each(&TemplateCache.delete/1)

      ConCache.delete(:templates, :all)

      Redix.del!("template:events")
    end)

    [templates: templates]
  end

  test "put/1 returns Ok", context do
    [template | _] = context[:templates]
    assert {:ok, [6, 1, 1]} == TemplateCache.put(template)
  end

  test "put/1 returns updates only when updated at is changed", context do
    [template | _] = context[:templates]
    assert {:ok, [6, 1, 1]} == TemplateCache.put(template)
    assert {:ok, []} == TemplateCache.put(template)
  end

  test "put/1 emits an event when a new template is cached", context do
    [template | _] = context[:templates]
    assert {:ok, [6, 1, 1]} == TemplateCache.put(template)

    assert {:ok, [event]} = Stream.read(:redix, ["template:events"], transform: true)
    assert event.event == "template_updated"
    assert event.template == "template:#{template.id}"
    assert event.subscope == "Subscope #{template.id}"
  end

  test "put/2 suppresses events if publish option is false", context do
    [template | _] = context[:templates]
    assert {:ok, [6, 1, 1]} == TemplateCache.put(template, publish: false)
  end

  test "put/1 deletes previous names in name_to_id_map", %{templates: templates} do
    [%{id: id, name: name} = template | _] = templates
    id = to_string(id)

    for name <- ["foo", "bar", "baz"] do
      assert {:ok, 1} = Redix.command(["HSET", @name_to_id_key, name, id])
    end

    assert {:ok, [3, _, _, _]} = TemplateCache.put(template)
    assert {:ok, [^name, ^id]} = Redix.command(["HGETALL", @name_to_id_key])
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
    assert t.subscope == template.subscope
    assert t.updated_at == to_string(template.updated_at)
  end

  test "get_by_name gets template, nil subscope converted to empty string" do
    template = random_template(false)
    TemplateCache.put(template)
    {:ok, t} = TemplateCache.get_by_name(template.name)
    assert t.content == template.content
    assert t.id == template.id
    assert t.name == template.name
    assert t.scope == template.scope
    assert t.subscope == ""
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

  test "fields_by_type!/2 returns fields by type", %{templates: [template | _]} do
    %{name: name, scope: scope} = template
    TemplateCache.put(template)
    assert TemplateCache.fields_by_type!(scope, "string") == %{name => ["field"]}
  end

  defp random_template(subscope?) do
    id = System.unique_integer([:positive])

    field_group = %{
      "name" => "group_name",
      "fields" => [%{"name" => "field", "type" => "string"}]
    }

    %{
      id: id,
      name: "Template #{id}",
      label: "Label #{id}",
      scope: "Scope #{id}",
      subscope: "Subscope #{id}",
      content: [field_group],
      updated_at: DateTime.utc_now()
    }
    |> maybe_put_subscope(subscope?)
  end

  defp maybe_put_subscope(%{id: event_id} = event, true) do
    Map.put(event, :subscope, "Subscope #{event_id}")
  end

  defp maybe_put_subscope(event, false) do
    Map.put(event, :subscope, nil)
  end

end
