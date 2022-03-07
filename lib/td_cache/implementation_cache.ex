defmodule TdCache.ImplementationCache do
  @moduledoc """
  Shared cache for implementations.
  """

  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.RuleCache
  alias TdCache.Utils.MapHelpers
  # alias TdCache.SystemCache

  ## Client API

  @doc """
  Creates cache entries relating to a given implementation.
  """
  def put(implementation, opts \\ []) do
    put_implementation(implementation, opts)
  end

  @doc """
  Reads implementation information for a given id from cache.
  """
  def get(id) do
    implementation = read_implementation(id)
    {:ok, implementation}
  end

  @doc """
  Reads implementation domain_id relating to a given implementation id.
  """
  def get_domain_id(id) do
    {:ok, domain_id} = Redix.command(["HGET", "implementation:#{id}", "domain_id"])
    domain_id
  end

  @doc """
  Deletes cache entries relating to a given implementation id.
  """
  def delete(id) do
    delete_implementation(id)
  end

  @doc """
  Returns the ids of referenced implementations that have been deleted.
  """
  @spec deleted_ids :: [integer()]
  def deleted_ids do
    ["SMEMBERS", "inmplementation:deleted_ids"]
    |> Redix.command!()
    |> Enum.map(&String.to_integer/1)
  end

  @doc """
  Returns a list of implementations ids referenced by links
  """
  @spec referenced_ids :: [integer()]
  def referenced_ids do
    LinkCache.referenced_ids("implementation:")
  end

  def clean_cached_implementations(keep_ids) do
    keep_key = "_implementation:keys:keep:#{System.os_time(:millisecond)}"

    ids_to_delete =
      ["SMEMBERS", "implementation:keys"]
      |> Redix.command!()
      |> Enum.map(fn "implementation:" <> id -> String.to_integer(id) end)
      |> Enum.reject(&(&1 in keep_ids))

    keep_cmds =
      keep_ids
      |> Enum.map(&"implementation:#{&1}")
      |> Enum.chunk_every(1000)
      |> Enum.map(&["SADD", keep_key | &1])

    del_cmds =
      ids_to_delete
      |> Enum.flat_map(&["implementation:#{&1}", "implementation:#{&1}:execution_result_info"])
      |> Enum.chunk_every(1000)
      |> Enum.map(&["DEL" | &1])

    [
      keep_cmds,
      del_cmds,
      [
        ["SINTERSTORE", "implementation:keys", "implementation:keys", keep_key],
        ["DEL", keep_key]
      ]
    ]
    |> Enum.concat()
    |> Redix.transaction_pipeline!()

    Enum.count(ids_to_delete)
  end

  ## Private functions

  @props [
    {:id, :integer},
    {:deleted_at, :datetime},
    {:domain_id, :integer},
    {:rule_id, :integer},
    {:goal, :float},
    {:implementation_key, :string},
    {:minimum, :float},
    {:result_type, :string},
    {:updated_at, :datetime}
  ]

  @rule_props [
    {:id, :integer},
    {:name, :string}
  ]

  @result_props [
    {:errors, :integer},
    {:records, :integer},
    {:result, :decimal},
    {:date, :datetime},
    {:result_text, :string}
  ]

  defp read_implementation(id) do
    case Redix.read_map("implementation:#{id}") do
      {:ok, nil} ->
        nil

      {:ok, implementation} ->
        execution_result_info =
          "implementation:#{id}:execution_result_info"
          |> Redix.read_map()
          # TdCache github runner doesn't have Elixir 13
          # |> then(fn {:ok, result} -> result end)
          |> case do
            {:ok, result} -> result
          end
          |> MapHelpers.parse_fields(@result_props)

        rule =
          implementation
          |> maybe_get_rule()
          # |> then(fn {:ok, rule} -> rule end)
          |> case do
            {:ok, result} -> result
          end
          |> MapHelpers.parse_fields(@rule_props)

        implementation
        |> MapHelpers.parse_fields(@props)
        |> put_optional(:execution_result_info, execution_result_info)
        |> put_optional(:rule, rule)
    end
  end

  defp maybe_get_rule(%{rule_id: rule_id}) when not is_nil(rule_id),
    do: RuleCache.get(String.to_integer(rule_id))

  defp maybe_get_rule(_), do: {:ok, nil}

  def put_optional(map, _key, nil), do: map
  def put_optional(map, key, value), do: Map.put(map, key, value)

  defp delete_implementation(id) do
    Redix.transaction_pipeline([
      ["DEL", "implementation:#{id}", "implementation:#{id}:execution_result_info"],
      ["SREM", "implementation:keys", "implementation:#{id}"],
      ["SADD", "implementation:deleted_ids", "#{id}"]
    ])
  end

  defp put_implementation(
         %{id: id, updated_at: updated_at, deleted_at: deleted_at} = implementation,
         opts
       ) do
    [last_updated, last_deleted] =
      Redix.command!(["HMGET", "implementation:#{id}", :updated_at, :deleted_at])

    implementation
    |> Map.put(:updated_at, "#{updated_at}")
    |> Map.put(:deleted_at, "#{deleted_at}")
    |> put_implementation(last_updated, last_deleted, opts[:force])
  end

  defp put_implementation(%{} = implementation, opts) do
    implementation
    |> Map.put_new(:deleted_at, nil)
    |> put_implementation(opts)
  end

  defp put_implementation(%{updated_at: ts, deleted_at: ds}, ts, ds, false), do: {:ok, []}

  defp put_implementation(%{updated_at: ts, deleted_at: ds}, ts, ds, nil), do: {:ok, []}

  defp put_implementation(implementation, _last_updated, _last_deleted, _force) do
    implementation
    |> implementation_commands()
    |> Redix.transaction_pipeline()
  end

  defp implementation_commands(%{id: id} = implementation) do
    props_keys = Enum.map(@props, fn {key, _} -> key end)

    implementation_props =
      implementation
      |> Map.take(props_keys)

    result_props_keys = Enum.map(@result_props, fn {key, _} -> key end)

    execution_result_info_props =
      implementation
      |> Map.get(:execution_result_info)
      |> case do
        result = %{} -> Map.take(result, result_props_keys)
        _ -> %{}
      end

    maybe_put_rule(implementation)

    [
      ["HSET", "implementation:#{id}", implementation_props],
      ["HSET", "implementation:#{id}:execution_result_info", execution_result_info_props],
      ["SADD", "implementation:keys", "implementation:#{id}"],
      refresh_deleted_ids_command(implementation)
    ]
  end

  defp maybe_put_rule(%{rule: rule}) when not is_nil(rule) do
    RuleCache.put(rule)
  end

  defp maybe_put_rule(_), do: nil

  defp refresh_deleted_ids_command(%{id: id} = implementation) do
    case Map.get(implementation, :deleted_at) do
      nil -> ["SREM", "implementation:deleted_ids", id]
      "" -> ["SREM", "implementation:deleted_ids", id]
      _ -> ["SADD", "implementation:deleted_ids", id]
    end
  end
end
