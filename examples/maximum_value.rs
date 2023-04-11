use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::ColumnIdentifier::{Custom, Dst, Id, Src};
use pregel_rs::pregel::{MessageReceiver, Pregel, PregelBuilder};
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
        Src.as_ref() => [0, 1, 1, 2, 2, 3],
        Dst.as_ref() => [1, 0, 3, 1, 3, 2],
    ]?;

    let vertices = df![
        Id.as_ref() => [0, 1, 2, 3],
        Custom("value".to_owned()).as_ref() => [3, 6, 2, 1],
    ]?;

    let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges)?)
        .max_iterations(4)
        .with_vertex_column(Custom("max_value".to_owned()))
        .initial_message(col(Custom("value".to_owned()).as_ref()))
        .send_messages(
            MessageReceiver::Dst,
            Pregel::src(Custom("max_value".to_owned())),
        )
        .aggregate_messages(Pregel::msg(None).max())
        .v_prog(max_exprs([
            col(Custom("max_value".to_owned()).as_ref()),
            Pregel::msg(None),
        ]))
        .build();

    Ok(println!("{}", pregel.run()?))
}
