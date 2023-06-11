use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::Column::{Custom, Object, Subject, VertexId};
use pregel_rs::pregel::{Column, MessageReceiver, PregelBuilder};
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
        Subject.as_ref() => [0, 0, 1, 2, 3, 4, 4, 4],
        Object.as_ref() => [1, 2, 2, 3, 3, 1, 2, 3],
    ]?;

    let vertices = GraphFrame::from_edges(edges.clone())?.out_degrees()?;

    let damping_factor = 0.85;
    let num_vertices: f64 = vertices.column(VertexId.as_ref())?.len() as f64;

    let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges)?)
        .max_iterations(4)
        .with_vertex_column(Custom("rank"))
        .initial_message(lit(1.0 / num_vertices))
        .send_messages(
            MessageReceiver::Subject,
            Column::subject(Column::Custom("rank")) / Column::subject(Column::Custom("out_degree")),
        )
        .send_messages(
            MessageReceiver::Object,
            Column::subject(Custom("rank")) / Column::subject(Custom("out_degree")),
        )
        .aggregate_messages(Column::msg(None).sum())
        .v_prog(
            Column::msg(None) * lit(damping_factor) + lit((1.0 - damping_factor) / num_vertices),
        )
        .build();

    Ok(println!("{}", pregel.run()?))
}
