defmodule TdCache.MixProject do
  use Mix.Project

  def project do
    [
      app: :td_cache,
      version: "6.16.0",
      elixir: "~> 1.12",
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
      {:assertions, "~> 0.19", only: :test},
      {:con_cache, "~> 1.0"},
      {:decimal, "~> 1.6 or ~> 2.0"},
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: :dev, runtime: false},
      {:ex_machina, "~> 2.4", only: :test},
      {:graph, git: "https://github.com/Bluetab/graph.git", tag: "1.3.0"},
      {:jason, "~> 1.1"},
      {:redix, "~> 1.0"}
    ]
  end
end
