defmodule TdCache.Redix.Pool do
  @moduledoc """
  A simple connection pool for Redix.
  """

  @pool_size 20

  def child_spec(config) do
    redis_host = Keyword.get(config, :redis_host, "redis")

    # Specs for the Redix connections.
    children =
      for i <- 0..(@pool_size - 1) do
        Supervisor.child_spec({Redix, host: redis_host, name: :"redix_#{i}"}, id: {Redix, i})
      end

    # Spec for the supervisor that will supervise the Redix connections.
    %{
      id: RedixSupervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end

  def command(command) do
    Redix.command(:"redix_#{random_index()}", command)
  end

  def command!(command) do
    Redix.command!(:"redix_#{random_index()}", command)
  end

  def transaction_pipeline(commands) do
    Redix.transaction_pipeline(:"redix_#{random_index()}", commands)
  end

  def transaction_pipeline!(commands) do
    Redix.transaction_pipeline!(:"redix_#{random_index()}", commands)
  end

  defp random_index do
    rem(System.unique_integer([:positive]), @pool_size)
  end
end
