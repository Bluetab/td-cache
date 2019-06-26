defmodule TdCache.TemplateCache do
  @moduledoc """
  Shared cache for form templates.
  """

  use GenServer

  alias Jason, as: JSON
  alias TdCache.Redix, as: Redis
  alias TdCache.Redix.Commands

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  def get(id, prop) do
    GenServer.call(__MODULE__, {:get, id, prop})
  end

  def get_by_name(name) do
    GenServer.call(__MODULE__, {:get_by_name, name})
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
    GenServer.call(__MODULE__, {:list, scope})
  end

  def list_by_scope!(scope) do
    case list_by_scope(scope) do
      {:ok, templates} -> templates
      error -> error
    end
  end

  def put(template) do
    GenServer.call(__MODULE__, {:put, template})
  end

  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  ## Callbacks

  @impl true
  def init(_args) do
    state = %{}
    {:ok, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    template = read_template(id)
    {:reply, {:ok, template}, state}
  end

  @impl true
  def handle_call({:get, id, prop}, _from, state) do
    value =
      id
      |> read_template
      |> Map.get(prop)

    {:reply, {:ok, value}, state}
  end

  @impl true
  def handle_call({:get_by_name, name}, _from, state) do
    template = read_by_name(name)
    {:reply, {:ok, template}, state}
  end

  @impl true
  def handle_call(:list, _from, state) do
    templates = list_templates()
    {:reply, {:ok, templates}, state}
  end

  @impl true
  def handle_call({:list, scope}, _from, state) do
    templates =
      list_templates()
      |> Enum.filter(&(Map.get(&1, :scope) == scope))

    {:reply, {:ok, templates}, state}
  end

  @impl true
  def handle_call({:put, template}, _from, state) do
    reply = put_template(template)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_template(id)
    {:reply, reply, state}
  end

  ## Private functions

  @props [:label, :scope, :name]
  @name_to_id_key "templates:name_to_id"

  defp read_template(id) when is_binary(id) do
    id
    |> String.to_integer()
    |> read_template
  end

  defp read_template(id) do
    case Redis.read_map("template:#{id}") do
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
    case Redis.command!(["HGET", @name_to_id_key, name]) do
      nil -> nil
      id -> read_template(id)
    end
  end

  defp put_template(%{id: id, name: name, content: content} = template) do
    template =
      template
      |> Map.take(@props)
      |> Map.put(:content, JSON.encode!(content))

    commands = [
      Commands.hmset("template:#{id}", template),
      ["HSET", @name_to_id_key, name, id]
    ]

    Redis.transaction_pipeline(commands)
  end

  defp list_templates do
    case Redis.read_map(@name_to_id_key) do
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
    commands =
      case Redis.command!(["HGET", "template:#{id}", :name]) do
        nil ->
          [["DEL", "template:#{id}"]]

        name ->
          [["DEL", "template:#{id}"], ["HDEL", @name_to_id_key, name]]
      end

    Redis.transaction_pipeline(commands)
  end
end
