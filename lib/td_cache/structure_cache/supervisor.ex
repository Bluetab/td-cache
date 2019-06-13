defmodule TdCache.StructureCache.Supervisor do
  @moduledoc """
  Supervisor for Structure Cache.
  """

  use Supervisor

  def start_link(options) do
    Supervisor.start_link(__MODULE__, options)
  end

  @impl true
  def init(options) do
    # List all child processes to be supervised
    children = [
      {TdCache.StructureCache, options}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
