defmodule TdCache.TemplateCache do
  @moduledoc """
  Shared cache for form templates.
  """
  use GenServer

  alias TdCache.EventStream.Publisher
  alias TdCache.Redix

  @props [:label, :scope, :subscope, :name, :updated_at]
  @name_to_id_key "templates:name_to_id"

  ## Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  def get(id, prop) do
    {:ok, template} = get(id)
    value = Map.get(template, prop)
    {:ok, value}
  end

  def get_by_name(name) do
    GenServer.call(__MODULE__, {:name, name})
  end

  def get_by_name!(name) do
    case get_by_name(name) do
      {:ok, template} -> template
      error -> error
    end
  end

  def list do
    GenServer.call(__MODULE__, :list)
  end

  def list! do
    case list() do
      {:ok, templates} -> templates
      error -> error
    end
  end

  def list_by_scope(scope) do
    case list() do
      {:ok, templates} ->
        {:ok, Enum.filter(templates, &(Map.get(&1, :scope) == scope))}

      error ->
        error
    end
  end

  def list_by_scope!(scope) do
    case list_by_scope(scope) do
      {:ok, templates} -> templates
      error -> error
    end
  end

  def fields_by_type!(scope, type) do
    scope
    |> list_by_scope!()
    |> Enum.map(fn t -> {t.name, content_fields(t, type)} end)
    |> Enum.reject(fn {_, v} -> Enum.empty?(v) end)
    |> Map.new()
  end

  @doc """
  Puts or updates a template in cache. Events may be suppressed by passing the
  option `publish: false`.
  """
  def put(template, opts \\ []) do
    GenServer.call(__MODULE__, {:put, template, opts})
  end

  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  ## Callbacks

  @impl true
  def init(_options) do
    {:ok, nil}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    template = read_template(id)
    {:reply, {:ok, template}, state}
  end

  @impl true
  def handle_call({:name, name}, _from, state) do
    template = get_cache(name, fn -> read_by_name(name) end)
    {:reply, {:ok, template}, state}
  end

  @impl true
  def handle_call(:list, _from, state) do
    templates = get_cache(:all, fn -> list_templates() end)
    {:reply, {:ok, templates}, state}
  end

  @impl true
  def handle_call({:put, %{id: id, name: name} = template, opts}, _from, state) do
    reply = put_template(template, opts)

    put_cache(name, read_template(id))
    put_cache(:all, list_templates())

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    case read_template(id) do
      nil ->
        :ok

      %{name: name} ->
        delete_cache(name)
    end

    reply = delete_template(id)

    put_cache(:all, list_templates())

    {:reply, reply, state}
  end

  ## Private functions

  defp get_cache(key, fun) do
    ConCache.isolated(:templates, key, nil, fn ->
      ConCache.get_or_store(:templates, key, fn -> fun.() end)
    end)
  end

  defp put_cache(key, value) do
    ConCache.isolated(:templates, key, nil, fn ->
      ConCache.put(:templates, key, value)
    end)
  end

  defp delete_cache(key) do
    ConCache.isolated(:templates, key, nil, fn ->
      ConCache.delete(:templates, key)
    end)
  end

  defp read_template(id) when is_binary(id) do
    id
    |> String.to_integer()
    |> read_template()
  end

  defp read_template(id) do
    case Redix.read_map("template:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, %{content: content} = template} ->
        template
        |> Map.put(:id, id)
        |> Map.put(:content, Jason.decode!(content))

      {:ok, template} ->
        Map.put(template, :id, id)
    end
  end

  defp read_by_name("template:" <> name) do
    read_by_name(name)
  end

  defp read_by_name(name) do
    case Redix.command!(["HGET", @name_to_id_key, name]) do
      nil -> nil
      id -> read_template(id)
    end
  end

  defp put_template(%{id: id, updated_at: updated_at} = template, opts) do
    last_updated = Redix.command!(["HGET", "template:#{id}", :updated_at])

    {force, opts} = Keyword.pop(opts, :force, false)

    template
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_template(last_updated, force, opts)
  end

  defp put_template(%{updated_at: ts}, ts, false, _opts), do: {:ok, []}

  defp put_template(
         %{id: id, name: name, content: content, scope: scope} = template,
         _last_updated,
         _force,
         opts
       ) do
    template =
      template
      |> Map.take(@props)
      |> Map.put(:content, Jason.encode!(content))

    commands =
      case prev_names(id, name) do
        [_ | _] = names -> [["HDEL", @name_to_id_key | names]]
        _ -> []
      end

    commands =
      commands ++
        [
          ["HSET", "template:#{id}", template],
          ["HSET", @name_to_id_key, name, id],
          ["SADD", "template:keys", "template:#{id}"]
        ]

    {:ok, results} = Redix.transaction_pipeline(commands)

    if Keyword.get(opts, :publish, true) do
      event =
        %{
          event: "template_updated",
          template: "template:#{id}",
          scope: scope
        }
        |> maybe_put_subscope(template)

      {:ok, _event_id} = Publisher.publish(event, "template:events")
    end

    {:ok, results}
  end

  defp maybe_put_subscope(event, %{subscope: subscope} = _template) do
    Map.put(event, :subscope, subscope)
  end

  defp maybe_put_subscope(event, _template) do
    event
  end

  defp list_templates do
    case Redix.read_map(@name_to_id_key) do
      {:ok, nil} ->
        []

      {:ok, map} ->
        map
        |> Map.values()
        |> Enum.uniq()
        |> Enum.map(&read_template/1)
        |> Enum.filter(& &1)
    end
  end

  defp prev_names(id, name) do
    id = to_string(id)

    case Redix.read_map(@name_to_id_key) do
      {:ok, nil} ->
        []

      {:ok, map} ->
        map
        |> Enum.filter(fn {k, v} -> v == id and k != name end)
        |> Enum.map(fn {k, _id} -> k end)
    end
  end

  defp delete_template(id) do
    case Redix.command!(["HGET", "template:#{id}", :name]) do
      nil ->
        Redix.transaction_pipeline([
          ["DEL", "template:#{id}"],
          ["SREM", "template:keys", "template:#{id}"]
        ])

      name ->
        Redix.transaction_pipeline([
          ["DEL", "template:#{id}"],
          ["HDEL", @name_to_id_key, name],
          ["SREM", "template:keys", "template:#{id}"]
        ])
    end
  end

  defp content_fields(%{content: content}, type) do
    content
    |> Enum.flat_map(& &1["fields"])
    |> Enum.filter(&(&1["type"] == type))
    |> Enum.map(& &1["name"])
  end

  defp content_fields(_, _), do: []
end
