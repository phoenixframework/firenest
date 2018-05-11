ExUnit.start(assert_receive_timeout: 2000)

nodes = [:"first@127.0.0.1", :"second@127.0.0.1", :"third@127.0.0.1"]
Firenest.Test.start_boot_server(hd(nodes))
Firenest.Test.start_firenest([hd(nodes)], adapter: Firenest.Topology.Erlang)

# Start other nodes async, so we can start running tests that don't need them right away
pid =
  spawn_link(fn ->
    receive do: (:continue -> :ok)
    Firenest.Test.spawn_nodes(tl(nodes))
    Firenest.Test.start_firenest(tl(nodes), adapter: Firenest.Topology.Erlang)
    Process.unregister(:firenest_topology_setup)
    Process.sleep(:infinity)
  end)

Process.register(pid, :firenest_topology_setup)
send(pid, :continue)
