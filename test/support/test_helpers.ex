defmodule Firenest.TestHelpers do
  @doc """
  Asserts process will hibernate within 10 seconds.
  """
  def assert_hibernate(pid) do
    wait_until(fn ->
      Process.info(pid, :current_function) === {:current_function, {:erlang, :hibernate, 3}}
    end)
  end

  @doc """
  Waits until fun is true `count * 10` milliseconds.
  """
  def wait_until(fun, count \\ 1000) do
    cond do
      count == 0 ->
        raise "waited until fun returned true but it never did"

      fun.() ->
        :ok

      true ->
        Process.sleep(10)
        wait_until(fun, count - 1)
    end
  end
end
