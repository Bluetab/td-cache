defmodule TdCache.MixProject do
  use Mix.Project

  def project do
    [
      app: :td_cache,
      version: "3.4.0",
      elixir: "~> 1.6",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :redix, :con_cache],
      mod: {TdCache.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:con_cache, "~> 0.13"},
      {:redix, "~> 0.8.2"},
      {:credo, "~> 1.0.0", only: [:dev, :test], runtime: false},
      {:jason, "~> 1.1"}
    ]
  end
end
