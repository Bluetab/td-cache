defmodule TdCache.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    redis_host = Application.get_env(:td_cache, :redis_host, "redis")

    children =
      [
        {TdCache.Redix.Pool, redis_host: redis_host},
        # Supervisor.child_spec({TdCache.EventStream.Consumer, config1}, id: :concept_event_stream),
        # Supervisor.child_spec({TdCache.EventStream.Consumer, config2}, id: :field_event_stream),
        TdCache.LinkCache,
        TdCache.FieldCache,
        TdCache.StructureCache,
        TdCache.SystemCache
      ] ++ cache_cleaner_workers() ++ event_stream_workers()

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: TdCache.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp event_stream_workers do
    case Application.get_env(:td_cache, :event_stream) do
      nil -> []
      config -> [{TdCache.EventStream, config}]
    end
  end

  defp cache_cleaner_workers do
    import Supervisor.Spec
    config = Application.get_env(:td_cache, :cache_cleaner, [])
    [worker(TdCache.CacheCleaner, [config])]
  end
end
