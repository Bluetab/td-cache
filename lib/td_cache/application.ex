defmodule TdCache.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    redis_host = Application.get_env(:td_cache, :redis_host)
    config1 = Application.get_env(:td_cache, :concept_event_stream)
    config2 = Application.get_env(:td_cache, :field_event_stream)

    children =
      [
        {Redix, host: redis_host, name: :redix},
        Supervisor.child_spec({TdCache.EventStream.Supervisor, config1}, id: :worker_1),
        Supervisor.child_spec({TdCache.EventStream.Supervisor, config2}, id: :worker_2),
        {TdCache.LinkCache.Supervisor, redis_host: redis_host},
        {TdCache.FieldCache.Supervisor, redis_host: redis_host},
        {TdCache.StructureCache.Supervisor, redis_host: redis_host},
        {TdCache.SystemCache.Supervisor, redis_host: redis_host}
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
