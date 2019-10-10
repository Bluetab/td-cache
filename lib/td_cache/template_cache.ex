defmodule TdCache.TemplateCache do
  @moduledoc """
  Shared cache for form templates.
  """
  use GenServer

  alias Jason, as: JSON
  alias TdCache.EventStream.Publisher
  alias TdCache.Redix

  @props [:label, :scope, :name, :updated_at]
  @name_to_id_key "templates:name_to_id"

  ## Client API

  def start_link(options \\ []) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
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

  def list!() do
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

  def put(template) do
    GenServer.call(__MODULE__, {:put, template})
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
  def handle_call({:put, %{id: id, name: name} = template}, _from, state) do
    reply = put_template(template)

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
    |> read_template
  end

  defp read_template(id) do
    case Redix.read_map("template:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, %{content: content} = template} ->
        template
        |> Map.put(:id, id)
        |> Map.put(:content, JSON.decode!(content))

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

  defp put_template(%{id: id, updated_at: updated_at} = template) do
    last_updated = Redix.command!(["HGET", "template:#{id}", :updated_at])

    template
    |> Map.put(:updated_at, "#{updated_at}")
    |> put_template(last_updated)
  end

  defp put_template(%{updated_at: ts}, ts), do: {:ok, []}

  defp put_template(
         %{id: id, name: name, content: content, scope: scope} = template,
         _last_updated
       ) do
    template =
      template
      |> Map.take(@props)
      |> Map.put(:content, JSON.encode!(content))

    commands = [
      ["HMSET", "template:#{id}", template],
      ["HSET", @name_to_id_key, name, id],
      ["SADD", "template:keys", "template:#{id}"]
    ]

    {:ok, results} = Redix.transaction_pipeline(commands)

    event = %{
      event: "template_updated",
      template: "template:#{id}",
      scope: scope
    }

    {:ok, _event_id} = Publisher.publish(event, "template:events")

    {:ok, results}
  end

  defp list_templates do
    case Redix.read_map(@name_to_id_key) do
      {:ok, nil} ->
        []

      {:ok, map} ->
        map
        |> Map.values()
        |> Enum.map(&read_template/1)
        |> Enum.filter(& &1)
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
    |> Enum.filter(&(&1["type"] == type))
    |> Enum.map(& &1["name"])
  end

  defp content_fields(_, _), do: []
end
