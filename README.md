# Firenest

Firenest is a library of components for building distributed systems.

All components are built on top of a replaceable topology that abstracts
operations:

  * `Firenest.Topology` - the core of Firenest which provides node
    discovery, failure handling, broadcast and message passing between
    nodes
  * `Firenest.PubSub` - a distributed and scalable PubSub implementation
  
## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `firenest` to your list of dependencies in `mix.exs`:

  ```elixir
  def deps do
    [{:firenest, "~> 0.1.0"}]
  end
  ```

  2. Ensure `firenest` is started before your application:

  ```elixir
  def application do
    [applications: [:firenest]]
  end
  ```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/firenest](https://hexdocs.pm/firenest).

## Contributing

To talk about development of Firenest, you can join the [##firenest](http://webchat.freenode.net/?channels=##firenest) channel on [freenode](https://freenode.net/).
