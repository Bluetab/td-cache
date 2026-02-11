defmodule TdCache.MockFailingRedix do
  @moduledoc """
  Mock module for testing LinkCache put_many function with failing Redis connections
  """

  def transaction_pipeline(_commands) do
    {:error, :connection_error}
  end
end

defmodule TdCache.BatchFailingRedix do
  @moduledoc """
  Mock module for testing LinkCache put_many function with batch failures
  """

  def transaction_pipeline(commands) do
    current_count = Process.get(:batch_count, 0)
    Process.put(:batch_count, current_count + 1)

    if current_count == 1 do
      {:error, :batch_2_failure}
    else
      generate_mock_results(commands)
    end
  end

  defp generate_mock_results(commands) do
    num_results = length(commands)

    mock_results =
      Enum.map(1..num_results, fn _ ->
        case :rand.uniform(3) do
          1 -> 1
          2 -> "OK"
          3 -> {:ok, "result"}
        end
      end)

    {:ok, mock_results}
  end
end

defmodule TdCache.ConditionalFailingRedix do
  @moduledoc """
  Mock module for testing LinkCache put_many function with conditional failures
  """

  def transaction_pipeline(_commands) do
    current_count = Process.get(:batch_count, 0)
    Process.put(:batch_count, current_count + 1)

    case current_count do
      1 ->
        {:error, :conditional_failure}

      _ ->
        {:ok, [1, 1, 1, 1, 1, 1]}
    end
  end
end

defmodule TdCache.ResultProcessingErrorRedix do
  @moduledoc """
  Mock module for testing LinkCache put_many function with result processing errors
  """

  def transaction_pipeline(commands) do
    results =
      Enum.map(1..length(commands), fn idx ->
        if idx <= 7 do
          1
        else
          0
        end
      end)

    if length(commands) > 7 do
      {:ok, results}
    else
      {:ok, Enum.map(commands, fn _ -> 1 end)}
    end
  end
end

defmodule TdCache.PostInsertValidationErrorRedix do
  @moduledoc """
  Mock module for testing LinkCache put_many function with post-insert validation errors
  """

  def transaction_pipeline(commands) do
    Enum.map(1..length(commands), fn _index ->
      case :rand.uniform(5) do
        1 -> 0
        _ -> 1
      end
    end)
    |> then(&{:ok, &1})
  end
end