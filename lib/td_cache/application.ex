defmodule TdCache.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    redis_host = Application.get_env(:td_cache, :redis_host)

    # List all child processes to be supervised
    children = [
      {Redix, host: redis_host, name: :redix}
      # Starts a worker by calling: TdCache.Worker.start_link(arg)
      # {TdCache.Worker, arg},
    ] ++ cache_cleaner_workers()

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: TdCache.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp cache_cleaner_workers do
    import Supervisor.Spec
    config = Application.get_env(:td_cache, :cache_cleaner, [])
    [worker(TdCache.CacheCleaner, [config])]
  end

end
