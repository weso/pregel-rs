# `pregel-rs`

[![CI](https://github.com/angelip2303/pregel-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/angelip2303/pregel-rs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/angelip2303/pregel-rs/branch/main/graph/badge.svg?token=8SCDSSPH13)](https://codecov.io/gh/angelip2303/pregel-rs)
[![latest_version](https://img.shields.io/crates/v/pregel-rs)](https://crates.io/crates/pregel-rs)
[![documentation](https://img.shields.io/docsrs/pregel-rs/latest)](https://docs.rs/pregel-rs/latest/pregel_rs/)

`pregel-rs` is a Graph processing library written in Rust that features
a Pregel-based Framework for implementing your own algorithms in a 
message-passing fashion. It is designed to be efficient and scalable, 
making it suitable for processing large-scale graphs.

## Features

- _Pregel-based framework_: `pregel-rs` is a powerful graph processing model
that allows users to implement graph algorithms in a message-passing fashion,
where computation is performed on vertices and messages are passed along edges.
`pregel-rs` provides a framework that makes it easy to implement graph 
algorithms using this model.

- _Rust-based implementation_: `pregel-rs` is implemented in Rust, a systems 
programming language known for its safety, concurrency, and performance.
Rust's strong type system and memory safety features help ensure that `pregel-rs`
is robust and reliable.

- _Efficient and scalable_: `pregel-rs` designed to be efficient and scalable,
making it suitable for processing large-scale graphs. It uses parallelism and
optimization techniques to minimize computation and communication overhead,
allowing it to handle graphs with millions or even billions of vertices and edges.
For us to achieve this, we have built it on top of [polars](https://github.com/pola-rs/polars)
a blazingly fast DataFrames library implemented in Rust using Apache Arrow
Columnar Format as the memory model.

- _Graph abstraction_: `pregel-rs` provides a graph abstraction that makes 
it easy to represent and manipulate graphs in Rust. It supports both directed and
undirected graphs, and provides methods for adding, removing, and querying vertices
and edges.

- _Customizable computation_: `pregel-rs` allows users to implement their own
computation logic by defining vertex computation functions. This gives users the 
flexibility to implement their own graph algorithms and customize the behavior
of `pregel-rs` to suit their specific needs.

## Getting started

To get started with `pregel-rs`, you can follow these steps:

1. _Install Rust_: `pregel-rs` requires Rust to be installed on your system.
You can install Rust by following the instructions on the official Rust website:
https://www.rust-lang.org/tools/install

2. _Create a new Rust project_: Once Rust is installed, you can create a new Rust
project using the Cargo package manager, which is included with Rust. You can
create a new project by running the following command in your terminal:

```sh
cargo new my_pregel_project
```

3. _Add `pregel-rs` as a dependency_: Next, you need to add `pregel-rs` as a 
dependency in your `Cargo.toml` file, which is located in the root directory
of your project. You can add the following line to your `Cargo.toml` file:

```toml
[dependencies]
pregel-rs = "0.0.10"
```

4. _Implement your graph algorithm_: Now you can start implementing your graph
algorithm using the `pregel-rs` framework. You can define your vertex computation
functions and use the graph abstraction provided by `pregel-rs` to manipulate the graph.

5. _Build and run your project_: Once you have implemented your graph algorithm, you
can build and run your project using the Cargo package manager. You can build your
project by running the following command in your terminal:

```sh
cargo build
```

And you can run your project by running the following command:

```sh
cargo run
```

## Acknowledgments

Read [Pregel: A System for Large-Scale Graph Processing](https://15799.courses.cs.cmu.edu/fall2013/static/papers/p135-malewicz.pdf)
for a reference on how to implement your own Graph processing algorithms in a Pregel fashion. If you want to take some 
inspiration from some curated-sources, just explore the [/examples](https://github.com/angelip2303/graph-rs/tree/main/examples)
folder of this repository.

## Related projects

1. [GraphX](https://github.com/apache/spark/tree/master/graphx) is a library enabling Graph processing in the context of 
Apache Spark.
2. [GraphFrames](https://github.com/graphframes/graphframes) is the DataFrame-based equivalent to GraphX.

## License

Copyright &copy; 2023 Ángel Iglesias Préstamo (<angel.iglesias.prestamo@gmail.com>)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

**By contributing to this project, you agree to release your
contributions under the same license.**
