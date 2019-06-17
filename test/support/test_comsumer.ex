defmodule TdCache.EventStream.TestConsumer do
  @moduledoc """
  A stream consumer for testing.
  """

  @behaviour TdCache.EventStream.Consumer

  use GenServer

  ## Client API

  def start_link(options) do
    GenServer.start_link(__MODULE__, options, name: __MODULE__)
  end

  @impl true
  def consume(events) do
    GenServer.cast(__MODULE__, {:consume, events})
  end

  ## Callbacks

  @impl true
  def init(config) do
    parent = config[:parent]
    {:ok, parent}
  end

  @impl true
  def handle_cast({:consume, events}, state) do
    parent = state
    send(parent, {:consumed, events})
    {:noreply, state}
  end
end
