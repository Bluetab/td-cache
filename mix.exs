defmodule TdCache.MixProject do
  use Mix.Project

  def project do
    [
      app: :td_cache,
      version: "7.7.0",
      elixir: "~> 1.18",
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
      {:assertions, "~> 0.20", only: :test},
      {:con_cache, "~> 1.1"},
      {:decimal, "~> 2.3"},
      {:credo, "~> 1.7.11", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:ex_machina, "~> 2.8", only: :test},
      {:graph, git: "https://github.com/Bluetab/graph.git", tag: "1.4.0"},
      {:jason, "~> 1.4"},
      {:redix, "~> 1.5"}
    ]
  end
end
