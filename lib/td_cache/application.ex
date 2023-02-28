defmodule TdCache.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    redis_host = Application.get_env(:td_cache, :redis_host, "redis")
    port = Application.get_env(:td_cache, :port, 6379)
    password = Application.get_env(:td_cache, :password)

    children =
      [
        {Redix, host: redis_host, port: port, password: password, name: :redix},
        {TdCache.Redix.Pool, redis_host: redis_host, port: port, password: password},
        TdCache.ConceptCache,
        TdCache.HierarchyCache,
        TdCache.TaxonomyCache,
        TdCache.TemplateCache,
        TdCache.UserCache,
        con_cache_child_spec(:templates, 10, 60),
        con_cache_child_spec(:hierarchies, 10, 60),
        con_cache_child_spec(:users, 10, 60),
        con_cache_child_spec(:taxonomy, 10, 60),
        con_cache_child_spec(:concepts, 10, 60)
      ] ++ event_stream_workers(redis_host, port, password)

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: TdCache.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp event_stream_workers(redis_host, port, password) do
    case Application.get_env(:td_cache, :event_stream) do
      nil ->
        []

      config ->
        [
          {TdCache.EventStream,
           Keyword.merge([redis_host: redis_host, port: port, password: password], config)}
        ]
    end
  end

  defp con_cache_child_spec(name, ttl, global_ttl) do
    Supervisor.child_spec(
      {ConCache,
       [
         name: name,
         ttl_check_interval: :timer.seconds(ttl),
         global_ttl: :timer.seconds(global_ttl)
       ]},
      id: {ConCache, name}
    )
  end
end
