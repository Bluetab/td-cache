defmodule TdCache.FieldCache do
  @moduledoc """
  Shared cache for links between entities.
  """
  use GenServer

  alias TdCache.Redix, as: Redis
  alias TdCache.StructureCache

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @doc """
  Creates cache entries relating to a given field.
  """
  def put(field) do
    GenServer.call(__MODULE__, {:put, field})
  end

  @doc """
  Reads field information from cache.
  """
  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  @doc """
  Deletes cache entries relating to a given field.
  """
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
  def handle_call({:put, field}, _from, state) do
    reply = put_field(field)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    field = read_field(id)
    {:reply, {:ok, field}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = delete_field(id)
    {:reply, reply, state}
  end

  ## Private functions

  defp read_field(id) do
    field_key = "data_field:#{id}"

    {:ok, field} = Redis.read_map(field_key)

    case field_entry_to_map(field) do
      nil -> nil
      m -> Map.put(m, :id, id)
    end
  end

  defp field_entry_to_map(nil), do: nil

  defp field_entry_to_map(field) do
    structure =
      case Map.get(field, :structure_id) do
        nil ->
          nil

        id ->
          {:ok, s} = StructureCache.get(id)
          s
      end

    field
    |> Map.merge(structure)
  end

  defp delete_field(id) do
    key = "data_field:#{id}"
    Redis.command(["DEL", key])
  end

  defp put_field(%{
         id: id,
         structure: %{id: structure_id} = structure
       }) do
    field_key = "data_field:#{id}"

    StructureCache.put(structure)

    Redis.command(["HMSET", field_key, "structure_id", "#{structure_id}"])
  end

  defp put_field(_) do
    {:error, :missing_structure}
  end
end
