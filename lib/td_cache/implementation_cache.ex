defmodule TdCache.ImplementationCache do
  @moduledoc """
  Shared cache for implementations.
  """

  alias TdCache.ConceptCache
  alias TdCache.LinkCache
  alias TdCache.Redix
  alias TdCache.RuleCache
  alias TdCache.Utils.MapHelpers

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
  def get(implementation_ref, opts \\ []) do
    implementation = read_implementation(implementation_ref, opts)
    {:ok, implementation}
  end

  @doc """
  List cached implementations ids.
  """
  def list, do: Redix.command!(["SMEMBERS", "implementation:keys"])

  @doc """
  Reads implementation domain_id relating to a given implementation id.
  """
  def get_domain_id(implementation_ref) do
    Redix.command!(["HGET", "implementation:#{implementation_ref}", "domain_id"])
  end

  @doc """
  Deletes cache entries relating to a given implementation id.
  """
  def delete(implementation_ref) do
    delete_implementation(implementation_ref)
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

  def delete_relation_impl_id_and_impl_ref do
    ["DEL", "relation_impl_id_to_impl_ref"]
    |> Redix.command!()
  end

  @doc """
  Returns a list of implementations ids referenced by links
  """
  @spec referenced_ids :: [integer()]
  def referenced_ids(type \\ "implementation") do
    LinkCache.referenced_ids("#{type}:")
  end

  def put_relation_impl_id_and_impl_ref([_ | _] = list_references) do
    Redix.command!(["HSET", "relation_impl_id_to_impl_ref" | list_references])
  end

  def put_relation_impl_id_and_impl_ref(_), do: 0

  def get_relation_impl_id_and_impl_ref do
    Redix.command!(["HGETALL", "relation_impl_id_to_impl_ref"])
  end

  def clean_cached_implementations(keep_ids) do
    keep_key = "_implementation:keys:keep:#{System.os_time(:millisecond)}"

    ids_to_delete =
      ["SMEMBERS", "implementation:keys"]
      |> Redix.command!()
      |> Enum.map(fn "implementation:" <> implementation_ref ->
        String.to_integer(implementation_ref)
      end)
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
    {:implementation_ref, :integer},
    {:deleted_at, :datetime},
    {:domain_id, :integer},
    {:rule_id, :integer},
    {:goal, :float},
    {:implementation_key, :string},
    {:minimum, :float},
    {:result_type, :string},
    {:updated_at, :datetime},
    {:status, :string}
  ]

  @rule_props [
    {:id, :integer},
    {:name, :string}
  ]

  @result_props [
    {:errors, :integer},
    {:records, :integer},
    {:result, :decimal},
    {:goal, :float},
    {:minimum, :float},
    {:date, :datetime},
    {:result_text, :string}
  ]

  defp read_implementation(implementation_ref, opts) do
    case Redix.read_map("implementation:#{implementation_ref}") do
      {:ok, nil} ->
        nil

      {:ok, implementation} ->
        execution_result_info =
          "implementation:#{implementation_ref}:execution_result_info"
          |> Redix.read_map()
          |> case do
            {:ok, result} -> result
          end
          |> MapHelpers.parse_fields(@result_props)

        rule =
          implementation
          |> maybe_get_rule()
          |> case do
            {:ok, result} -> result
          end
          |> MapHelpers.parse_fields(@rule_props)

        concepts = get_concepts_links(implementation_ref, opts)

        implementation
        |> MapHelpers.parse_fields(@props)
        |> put_optional(:execution_result_info, execution_result_info)
        |> put_optional(:rule, rule)
        |> put_optional(:concepts_links, concepts)
    end
  end

  defp maybe_get_rule(%{rule_id: ""}), do: {:ok, nil}

  defp maybe_get_rule(%{rule_id: rule_id}) when is_binary(rule_id) do
    case Integer.parse(rule_id) do
      {id, ""} -> RuleCache.get(id)
      _ -> {:ok, nil}
    end
  end

  defp maybe_get_rule(_), do: {:ok, nil}

  defp get_concepts_links(implementation_ref, opts) do
    ["SMEMBERS", "implementation_ref:#{implementation_ref}:links:business_concept"]
    |> Redix.command!()
    |> Enum.map(fn link_id ->
      case LinkCache.get(link_id) do
        {:ok, nil} ->
          nil

        {:ok, %{target: "business_concept:" <> concept_id}} ->
          get_concepts_links_names(concept_id, opts)
      end
    end)
    |> Enum.reject(&(&1 === nil))
  end

  defp get_concepts_links_names(concept_id, opts) do
    case ConceptCache.get(concept_id, opts) do
      {:ok, nil} ->
        nil

      {:ok, concept} ->
        concept
        |> Map.take([
          :name,
          :id
        ])
    end
  end

  def put_optional(map, _key, nil), do: map
  def put_optional(map, key, value), do: Map.put(map, key, value)

  defp delete_implementation(implementation_ref) do
    Redix.transaction_pipeline([
      [
        "DEL",
        "implementation:#{implementation_ref}",
        "implementation:#{implementation_ref}:execution_result_info"
      ],
      ["SREM", "implementation:keys", "implementation:#{implementation_ref}"],
      ["SADD", "implementation:deleted_ids", "#{implementation_ref}"]
    ])
  end

  defp put_implementation(
         %{implementation_ref: implementation_ref, updated_at: updated_at, deleted_at: deleted_at} =
           implementation,
         opts
       ) do
    [last_updated, last_deleted] =
      Redix.command!(["HMGET", "implementation:#{implementation_ref}", :updated_at, :deleted_at])

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

  defp implementation_commands(%{implementation_ref: implementation_ref} = implementation) do
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
      ["HSET", "implementation:#{implementation_ref}", implementation_props],
      [
        "HSET",
        "implementation:#{implementation_ref}:execution_result_info",
        execution_result_info_props
      ],
      ["SADD", "implementation:keys", "implementation:#{implementation_ref}"],
      refresh_deleted_ids_command(implementation)
    ]
  end

  defp maybe_put_rule(%{rule: rule, rule_id: rule_id}) when not is_nil(rule_id) do
    RuleCache.put(rule)
  end

  defp maybe_put_rule(_), do: nil

  defp refresh_deleted_ids_command(%{implementation_ref: implementation_ref} = implementation) do
    case Map.get(implementation, :deleted_at) do
      nil -> ["SREM", "implementation:deleted_ids", implementation_ref]
      "" -> ["SREM", "implementation:deleted_ids", implementation_ref]
      _ -> ["SADD", "implementation:deleted_ids", implementation_ref]
    end
  end
end
