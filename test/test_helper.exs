ExUnit.start(assert_receive_timeout: 2000)

nodes = [:"first@127.0.0.1", :"second@127.0.0.1", :"third@127.0.0.1"]
Firenest.Test.spawn(nodes, adapter: Firenest.Topology.Erlang)
