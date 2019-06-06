defmodule TdCache.CacheCleaner do
    @moduledoc """
    GenServer to eliminate deprecated or unused cache entries in Redis
    """
  
    use GenServer
    require Logger
  
    def start_link(config, name \\ __MODULE__) do
      GenServer.start_link(__MODULE__, config, name: name)
    end
  
    def clean do
      GenServer.cast(__MODULE__, :clean)
    end
  
    def ping do
      GenServer.call(__MODULE__, :ping)
    end
  
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
    def handle_call(:ping, _from, state) do
      {:reply, :pong, state}
    end
  
    @impl true
    def handle_cast(:clean, state) do
      patterns = Keyword.get(state, :patterns, [])
      clean_deprecated_entries(patterns)
      {:noreply, state}
    end
  
    defp clean_deprecated_entries(patterns) do
      patterns
      |> Enum.each(&clean_entries/1)
    end
  
    defp clean_entries(pattern) do
      {:ok, keys} = Redix.command(:redix, ["KEYS", pattern])
  
      unless Enum.empty?(keys) do
        {:ok, count} = Redix.command(:redix, ["DEL" | keys])
        Logger.info("Deleted #{count} cache entries for pattern '#{pattern}'")
      end
    end
  end
  