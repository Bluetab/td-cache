defmodule TdCache.CacheCleaner do
  @moduledoc """
  GenServer to eliminate deprecated or unused cache entries in Redis
  """

  use GenServer

  alias TdCache.Redix

  require Logger

  ## Client API

  def start_link(config, name \\ __MODULE__) do
    GenServer.start_link(__MODULE__, config, name: name)
  end

  def clean do
    GenServer.call(__MODULE__, :clean)
  end

  ## Callbacks

  @impl true
  def init(state) do
    if Keyword.get(state, :clean_on_startup, false) do
      Process.send_after(self(), :clean_on_startup, 0)
    end

    state = Keyword.drop(state, [:clean_on_startup])
    {:ok, state}
  end

  @impl true
  def handle_info(:clean_on_startup, state) do
    patterns = Keyword.get(state, :patterns, [])
    clean_deprecated_entries(patterns)
    {:noreply, state}
  end

  @impl true
  def handle_call(:clean, _from, state) do
    patterns = Keyword.get(state, :patterns, [])
    clean_deprecated_entries(patterns)
    {:reply, :ok, state}
  end

  ## Private functions

  defp clean_deprecated_entries(patterns) do
    Enum.each(patterns, &clean_entries/1)
  end

  defp clean_entries(pattern) do
    Redix.del!(pattern)
  end
end
