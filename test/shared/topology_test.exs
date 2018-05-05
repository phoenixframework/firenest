defmodule Firenest.TopologyTest do
  @moduledoc """
  Tests for the topology API.
  """

  # Must be sync because we are changing the topology.
  use ExUnit.Case
  alias Firenest.Topology, as: T

  import Firenest.TestHelpers

  setup_all do
    wait_until(fn -> Process.whereis(:firenest_topology_setup) == nil end)
    topology = Firenest.Test

    {:ok,
     topology: topology,
     evaluator: Firenest.Test.Evaluator,
     node: T.node(topology),
     nodes: T.nodes(topology)}
  end

  setup %{test: test} do
    # Register the current process to receive topology messages.
    Process.register(self(), test)
    :ok
  end

  describe "node/1" do
    test "returns the current node name", %{topology: topology} do
      assert {:"first@127.0.0.1", _} = T.node(topology)
    end
  end

  describe "nodes/1" do
    test "returns all connected nodes except self", %{topology: topology} do
      assert ["second@127.0.0.1": _, "third@127.0.0.1": _] = Enum.sort(T.nodes(topology))
    end
  end

  describe "send/4" do
    @describetag :broadcast

    test "messages name in the given node", config do
      %{topology: topology, evaluator: evaluator, test: test, node: first, nodes: [second, third]} =
        config

      cmd =
        quote do
          reply = {:reply, T.node(unquote(topology))}
          T.send(unquote(topology), unquote(first), unquote(test), reply)
        end

      assert T.send(topology, third, evaluator, {:eval_quoted, cmd}) == :ok

      assert_receive {:reply, ^third}
      refute_received {:reply, ^second}
      refute_received {:reply, ^first}
    end

    test "messages name in the current node", config do
      %{topology: topology, evaluator: evaluator, test: test, node: first, nodes: [second, third]} =
        config

      cmd =
        quote do
          reply = {:reply, T.node(unquote(topology))}
          T.send(unquote(topology), unquote(first), unquote(test), reply)
        end

      assert T.send(topology, first, evaluator, {:eval_quoted, cmd}) == :ok

      assert_receive {:reply, ^first}
      refute_received {:reply, ^second}
      refute_received {:reply, ^third}
    end

    test "returns error when messaging unknown node", config do
      %{topology: topology, evaluator: evaluator} = config
      node_ref = {:"unknown@127.0.0.1", 1}
      assert T.send(topology, node_ref, evaluator, :oops) == {:error, :noconnection}
    end

    test "returns error when messaging node with old ref", config do
      %{topology: topology, evaluator: evaluator, nodes: [{name, _ref} | _]} = config
      assert T.send(topology, {name, make_ref()}, evaluator, :oops) == {:error, :noconnection}
    end
  end

  describe "broadcast/3" do
    @describetag :broadcast

    test "messages name in all known nodes except self", config do
      %{topology: topology, evaluator: evaluator, test: test, node: first, nodes: [second, third]} =
        config

      # We broadcast a message to the evaluator in all nodes
      # and then evaluate code that broadcasts a message back
      # to the test process.
      cmd =
        quote do
          T.broadcast(unquote(topology), unquote(test), {:reply, T.node(unquote(topology))})
        end

      T.broadcast(topology, evaluator, {:eval_quoted, cmd})

      assert_receive {:reply, ^second}
      assert_receive {:reply, ^third}
      refute_received {:reply, ^first}
    end
  end

  describe "connection" do
    @describetag :connection

    @node :"subscribe@127.0.0.1"
    test "may be set and managed explicitly", %{topology: topology} do
      # No node yet
      refute T.disconnect(topology, @node)
      refute @node in Keyword.keys(T.nodes(topology))

      # Start the node but not firenest
      Firenest.Test.spawn_nodes([@node])
      refute @node in Keyword.keys(T.nodes(topology))

      # Finally start firenest
      Firenest.Test.start_firenest([@node], adapter: T.adapter!(topology))
      assert T.connect(topology, @node)
      assert @node in Keyword.keys(T.nodes(topology))

      # Connect should still return true
      assert T.connect(topology, @node)

      # Now let's diconnect
      assert T.disconnect(topology, @node)
      refute @node in Keyword.keys(T.nodes(topology))

      # And we can't connect it back because it is permanently down
      refute T.connect(topology, @node)
    after
      T.disconnect(topology, @node)
    end
  end

  describe "sync_named/2" do
    @describetag :sync_named

    test "raises when process is not named", config do
      %{topology: topology, test: test} = config
      Process.unregister(test)

      assert_raise ArgumentError, ~r/cannot sync process/, fn ->
        T.sync_named(topology, self())
      end
    end

    test "cannot sync the same name twice", config do
      %{topology: topology} = config
      assert T.sync_named(topology, self()) == {:ok, []}
      assert T.sync_named(topology, self()) == {:error, {:already_synced, self()}}
    end

    @node :"sync_named@127.0.0.1"
    test "links to synced process", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      Firenest.Test.spawn_nodes([@node])
      [node_ref] = Firenest.Test.start_firenest([@node], adapter: T.adapter!(topology))
      Firenest.Test.start_reporter([@node])

      assert {:ok, []} = T.sync_named(topology, self())
      start_sync_named_on(topology, node_ref, evaluator, test)
      assert_receive {:named_up, ^node_ref, ^test}

      cmd =
        quote do
          # Application.ensure_all_started(:sasl)
          ref = Process.monitor(unquote(test))
          Process.exit(Process.whereis(unquote(topology)), :kill)

          receive do
            {:DOWN, ^ref, _, _, _} ->
              Firenest.Test.report(:down_success)
          end
        end

      T.send(topology, node_ref, evaluator, {:eval_quoted, cmd})

      assert_receive :down_success
      assert_receive {:named_down, ^node_ref, ^test}
    end

    test "receives messages from nodes across the network", config do
      %{topology: topology, evaluator: evaluator, test: test, nodes: [second, third]} = config

      # We start sync named and make sure it is up
      start_sync_named_on(topology, second, evaluator, test)
      wait_until_at_least_one_sync_named(topology, test)
      assert {:ok, [^second]} = T.sync_named(topology, self())

      # Make another node sync when we are already synced
      start_sync_named_on(topology, third, evaluator, test)
      assert_receive {:named_up, ^third, ^test}

      # Let's bring yet another node up
      Firenest.Test.spawn_nodes([@node])
      [node_ref] = Firenest.Test.start_firenest([@node], adapter: T.adapter!(topology))
      start_sync_named_on(topology, node_ref, evaluator, test)
      assert_receive {:named_up, ^node_ref, ^test}

      # And now let's disconnect from it
      assert T.disconnect(topology, @node)
      assert_receive {:named_down, ^node_ref, ^test}

      # And now let's kill the named process running on third
      cmd = quote(do: Process.exit(Process.whereis(unquote(test)), :shutdown))
      T.send(topology, third, evaluator, {:eval_quoted, cmd})

      assert_receive {:named_down, ^third, ^test}
    end

    defp start_sync_named_on(topology, node, evaluator, name) do
      cmd =
        quote do
          Task.start(fn ->
            Process.register(self(), unquote(name))
            res = T.sync_named(unquote(topology), self())
            Process.sleep(:infinity)
          end)
        end

      T.send(topology, node, evaluator, {:eval_quoted, cmd})
    end

    defp wait_until_at_least_one_sync_named(topology, name) do
      Process.unregister(name)

      wait_until(fn ->
        fn ->
          Process.register(self(), name)
          T.sync_named(topology, self()) != {:ok, []}
        end
        |> Task.async()
        |> Task.await()
      end)

      Process.register(self(), name)
    end
  end
end
