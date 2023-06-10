use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::Column::{Custom, Object, Subject, VertexId};
use pregel_rs::pregel::{Column, MessageReceiver, PregelBuilder};
use std::error::Error;

/// This Rust function uses the Pregel algorithm to find the maximum value in a
/// graph.
///
/// Returns:
///
/// The `main` function is returning a `Result` with an empty `Ok` value and a `Box`
/// containing a `dyn Error` trait object. The `println!` macro is being used to
/// print the result of the `pregel.run()` method call to the console.
fn main() -> Result<(), Box<dyn Error>> {
    let edges = df![
        Subject.as_ref() => [0, 1, 1, 2, 2, 3],
        Object.as_ref() => [1, 0, 3, 1, 3, 2],
    ]?;

    let vertices = df![
        VertexId.as_ref() => [0, 1, 2, 3],
        Custom("value").as_ref() => [3, 6, 2, 1],
    ]?;

    let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges)?)
        .max_iterations(4)
        .with_vertex_column(Custom("max_value"))
        .initial_message(col(Custom("value").as_ref()))
        .send_messages(MessageReceiver::Dst, Column::src(Custom("max_value")))
        .aggregate_messages(Column::msg(None).max())
        .v_prog(max_exprs([
            col(Custom("max_value").as_ref()),
            Column::msg(None),
        ]))
        .build();

    Ok(println!("{}", pregel.run()?))
}
