**Note**: Work on firenest has been halted. We plan to incorporate some
of those features in Phoenix.PubSub instead.

# Firenest

Firenest is a library of components for building distributed systems.

All components are built on top of a replaceable topology that abstracts
operations:

  * `Firenest.Topology` - the core of Firenest which provides node
    discovery, failure handling, broadcast and message passing between
    nodes
  * `Firenest.PubSub` - a distributed and scalable PubSub implementation

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
