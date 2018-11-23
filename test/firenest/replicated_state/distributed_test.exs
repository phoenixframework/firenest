defmodule Firenest.ReplicatedState.DistributedTest do
  use ExUnit.Case

  alias Firenest.Topology, as: T
  alias Firenest.ReplicatedState, as: R

  import Firenest.TestHelpers

  setup_all do
    wait_until(fn -> Process.whereis(:firenest_topology_setup) == nil end, 5000)
    nodes = [:"first@127.0.0.1", :"second@127.0.0.1", :"third@127.0.0.1"]
    topology = Firenest.Test
    server = Firenest.Test.ReplicatedState

    %{start: start} =
      R.child_spec(name: server, topology: topology, handler: Firenest.Test.EvalState)

    Firenest.Test.start_link(nodes, start)
    nodes = for {name, _} = ref <- T.nodes(topology), name in nodes, do: ref

    {:ok, topology: topology, evaluator: Firenest.Test.Evaluator, nodes: nodes, server: server}
  end

  test "remote join is propagated", config do
    %{server: server, test: test, nodes: [second | _]} = config

    quote do
      spawn(fn ->
        :ok = R.put(unquote(server), unquote(test), self(), :baz)
        Process.sleep(:infinity)
      end)
    end
    |> eval_on_node(second, config)

    wait_until(fn -> R.list(server, test) == [:baz] end)
  end

  test "propages changes when nodes were disconnected", config do
    %{topology: topology, server: server, test: test, nodes: [second, third]} = config
    Process.register(self(), test)

    quote do
      spawn(fn ->
        Process.register(self(), unquote(test))
        :ok = R.put(unquote(server), unquote(test), self(), :baz)
        Process.sleep(:infinity)
      end)
    end
    |> eval_on_node(second, config)

    quote(do: R.list(unquote(server), unquote(test)) == [:baz])
    |> await_on_node(third, config)

    quote do
      T.disconnect(unquote(topology), elem(unquote(third), 0))
      pid = Process.whereis(unquote(test))
      :ok = R.delete(unquote(server), unquote(test), pid)

      spawn(fn ->
        :ok = R.put(unquote(server), unquote(test), self(), :bar)
        Process.sleep(:infinity)
      end)
    end
    |> eval_on_node(second, config)

    quote(do: R.list(unquote(server), unquote(test)) == [])
    |> await_on_node(third, config)

    quote(do: T.connect(unquote(topology), elem(unquote(third), 0)))
    |> eval_on_node(second, config)

    quote(do: R.list(unquote(server), unquote(test)) == [:bar])
    |> await_on_node(third, config)
  end

  defp eval_on_node(quoted, node, config) do
    %{topology: topology, evaluator: evaluator} = config

    T.send(topology, node, evaluator, {:eval_quoted, quoted})
  end

  defp await_on_node(quoted, node, config) do
    %{topology: topology} = config
    {:registered_name, name} = Process.info(self(), :registered_name)

    quote do
      wait_until(fn -> unquote(quoted) end)
      T.broadcast(unquote(topology), unquote(name), :continue)
    end
    |> eval_on_node(node, config)

    assert_receive :continue
  end
end
