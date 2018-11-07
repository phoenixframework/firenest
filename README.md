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

## License

Copyright 2016 Chris McCord and Plataformatec

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.