defmodule Firenest.TopologyTest do
  @moduledoc """
  Tests for the topology API.
  """

  # Must be sync because we are changing the topology.
  use ExUnit.Case
  alias Firenest.Topology, as: T

  setup %{test: test} do
    # Register the current process to receive topology messages.
    Process.register(self(), test)
    {:ok, topology: Firenest.Test, evaluator: Firenest.Test.Evaluator}
  end

  describe "node/1" do
    test "returns the current node name", %{topology: topology} do
      assert T.node(topology) == :"first@127.0.0.1"
    end
  end

  describe "nodes/1" do
    test "returns all connected nodes except self", %{topology: topology} do
      assert T.nodes(topology) |> Enum.sort ==
             [:"second@127.0.0.1", :"third@127.0.0.1"]
    end
  end

  describe "send/4" do
    @describetag :broadcast

    test "messages name in the given node", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      assert T.send(topology, :"third@127.0.0.1", evaluator, {:eval_quoted, quote do
        T.send(unquote(topology), :"first@127.0.0.1", unquote(test), {:reply, T.node(unquote(topology))})
      end}) == :ok

      assert_receive {:reply, :"third@127.0.0.1"}
      refute_received {:reply, :"second@127.0.0.1"}
      refute_received {:reply, :"first@127.0.0.1"}
    end

    test "messages name in the current node", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      assert T.send(topology, :"first@127.0.0.1", evaluator, {:eval_quoted, quote do
        T.send(unquote(topology), :"first@127.0.0.1", unquote(test), {:reply, T.node(unquote(topology))})
      end}) == :ok

      assert_receive {:reply, :"first@127.0.0.1"}
      refute_received {:reply, :"second@127.0.0.1"}
      refute_received {:reply, :"third@127.0.0.1"}
    end

    test "returns error when messaging unknown node", config do
      %{topology: topology, evaluator: evaluator} = config
      assert T.send(topology, :"unknown@127.0.0.1", evaluator, :oops) ==
             {:error, :noconnection}
    end
  end

  describe "broadcast/3" do
    @describetag :broadcast

    test "messages name in all known nodes except self", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      # We broadcast a message to the evaluator in all nodes
      # and then evaluate code that broadcasts a message back
      # to the test process.
      T.broadcast(topology, evaluator, {:eval_quoted, quote do
        T.broadcast(unquote(topology), unquote(test), {:reply, T.node(unquote(topology))})
      end})

      assert_receive {:reply, :"third@127.0.0.1"}
      assert_receive {:reply, :"second@127.0.0.1"}
      refute_received {:reply, :"first@127.0.0.1"}
    end
  end

  describe "connection" do
    @describetag :connection

    @node :"subscribe@127.0.0.1"
    test "may be set and managed explicitly", %{topology: topology} do
      # No node yet
      refute T.disconnect(topology, @node)
      refute @node in T.nodes(topology)

      # Start the node but not firenest
      Firenest.Test.spawn_nodes([@node])
      refute @node in T.nodes(topology)

      # Finally start firenest
      Firenest.Test.start_firenest([@node], adapter: T.adapter!(topology))
      assert T.connect(topology, @node)
      assert @node in T.nodes(topology)

      # Connect should still return true
      assert T.connect(topology, @node)

      # Now let's diconnect
      assert T.disconnect(topology, @node)
      refute @node in T.nodes(topology)

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
    test "receives messages from nodes across the network", config do
      %{topology: topology, evaluator: evaluator, test: test} = config

      # We start sync named and make sure it is up
      start_sync_named_on(topology, :"second@127.0.0.1", evaluator, test)
      wait_until_at_least_one_sync_named(topology, test)
      assert {:ok, [{:"second@127.0.0.1", _second_id}]} = T.sync_named(topology, self())

      # Make another node sync when we are already synced
      start_sync_named_on(topology, :"third@127.0.0.1", evaluator, test)
      assert_receive {:named_up, :"third@127.0.0.1", third_id, ^test}

      # Let's bring yet another node up
      Firenest.Test.spawn_nodes([@node])
      Firenest.Test.start_firenest([@node], adapter: T.adapter!(topology))
      start_sync_named_on(topology, @node, evaluator, test)
      assert_receive {:named_up, @node, node_id, ^test}

      # And now let's disconnect from it
      assert T.disconnect(topology, @node)
      assert_receive {:named_down, @node, ^node_id, ^test}

      # And now let's kill the named process running on third
      T.send(topology, :"third@127.0.0.1", evaluator, {:eval_quoted, quote do
        Process.exit(Process.whereis(unquote(test)), :shutdown)
      end})
      assert_receive {:named_down, :"third@127.0.0.1", ^third_id, ^test}
    end

    defp start_sync_named_on(topology, node, evaluator, name) do
      T.send(topology, node, evaluator, {:eval_quoted, quote do
        Task.start(fn ->
          Process.register(self(), unquote(name))
          T.sync_named(unquote(topology), self())
          Process.sleep(:infinity)
        end)
      end})
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

  defp wait_until(fun, count \\ 1000) do
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
