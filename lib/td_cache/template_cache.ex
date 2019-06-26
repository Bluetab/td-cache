defmodule TdCache.TemplateCache do
  @moduledoc """
  Shared cache for form templates.
  """

  alias Jason, as: JSON
  alias TdCache.Redix

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  def get(id) do
    template = read_template(id)
    {:ok, template}
  end

  def get(id, prop) do
    value =
      id
      |> read_template
      |> Map.get(prop)

    {:ok, value}
  end

  def get_by_name(name) do
    template = read_by_name(name)
    {:ok, template}
  end

  def get_by_name!(name) do
    case get_by_name(name) do
      {:ok, template} -> template
      error -> error
    end
  end

  def list do
    templates = list_templates()
    {:ok, templates}
  end

  def list!() do
    case list() do
      {:ok, templates} -> templates
      error -> error
    end
  end

  def list_by_scope(scope) do
    templates =
      list_templates()
      |> Enum.filter(&(Map.get(&1, :scope) == scope))

    {:ok, templates}
  end

  def list_by_scope!(scope) do
    case list_by_scope(scope) do
      {:ok, templates} -> templates
      error -> error
    end
  end

  def put(template) do
    put_template(template)
  end

  def delete(id) do
    delete_template(id)
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

  defp put_template(%{id: id, name: name, content: content} = template) do
    template =
      template
      |> Map.take(@props)
      |> Map.put(:content, JSON.encode!(content))

    commands = [
      ["HMSET", "template:#{id}", template],
      ["HSET", @name_to_id_key, name, id]
    ]

    Redix.transaction_pipeline(commands)
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
        Redix.command(["DEL", "template:#{id}"])

      name ->
        Redix.transaction_pipeline([
          ["DEL", "template:#{id}"],
          ["HDEL", @name_to_id_key, name]
        ])
    end
  end
end
