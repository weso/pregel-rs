use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::ColumnIdentifier::{Custom, Dst, Id, Src};
use pregel_rs::pregel::{MessageReceiver, Pregel, PregelBuilder};
use std::error::Error;

/// This Rust function implements the PageRank algorithm using the Pregel framework.
///
/// Returns:
///
/// The `main` function is returning a `Result` with an empty `Ok` value and a `Box`
/// containing a `dyn Error` trait object. The `println!` macro is used to print the
/// result of the `pregel.run()` method call to the console.
fn main() -> Result<(), Box<dyn Error>> {
    let edges = df![
        Src.as_ref() => [0, 0, 1, 2, 3, 4, 4, 4],
        Dst.as_ref() => [1, 2, 2, 3, 3, 1, 2, 3],
    ]?;

    let vertices = GraphFrame::from_edges(edges.clone())?.out_degrees()?;

    let damping_factor = 0.85;
    let num_vertices: f64 = vertices.column(Id.as_ref())?.len() as f64;

    let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges)?)
        .max_iterations(4)
        .with_vertex_column(Custom("rank".to_owned()))
        .initial_message(lit(1.0 / num_vertices))
        .send_messages(
            MessageReceiver::Dst,
            Pregel::src(Custom("rank".to_owned())) / Pregel::src(Custom("out_degree".to_owned())),
        )
        .aggregate_messages(Pregel::msg(None).sum())
        .v_prog(
            Pregel::msg(None) * lit(damping_factor) + lit((1.0 - damping_factor) / num_vertices),
        )
        .build();

    Ok(println!("{}", pregel.run()?))
}
