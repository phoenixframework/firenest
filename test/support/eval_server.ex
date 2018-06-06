defmodule Firenest.Test.EvalServer do
  use Firenest.SyncedServer

  def init(fun) when is_function(fun, 0), do: fun.()
  def init({:eval, cmd}), do: elem(Code.eval_quoted(cmd), 0)
  def init(state), do: {:ok, state}

  def handle_call(:state, _, state), do: {:reply, state, state}
  def handle_call(fun, from, state), do: fun.(from, state)

  def handle_info(:timeout, fun), do: fun.()
  def handle_info({:state, state}, _state), do: {:noreply, state}
  def handle_info(fun, state), do: fun.(:info, state)

  def handle_replica(status, replica, fun) when is_function(fun), do: fun.(status, replica)
  def handle_replica(_status, _replica, state), do: {:noreply, state + 1}

  def handle_remote(fun, from, state), do: fun.(from, state)

  def format_status(_, [pdict, state]) do
    case Keyword.get(pdict, :format_status) do
      nil -> state
      fun -> fun.(state)
    end
  end

  def terminate({:shutdown, fun}, state), do: fun.(state)
  def terminate({:abnormal, fun}, state), do: fun.(state)
  def terminate({{:nocatch, {:abnormal, fun}}, _}, state), do: fun.(state)
  def terminate({{:abnormal, fun}, _}, state), do: fun.(state)
end
