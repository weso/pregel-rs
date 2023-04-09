use std::error::Error;
use polars::prelude::*;
use graph_rs::graph_frame::{GraphFrame, ID};
use graph_rs::pregel::{MessageReceiver, Pregel, PregelBuilder};

fn main() -> Result<(), Box<dyn Error>> {
    let edges = df![
        "src" => [0, 0, 1, 2, 3, 4, 4, 4],
        "dst" => [1, 2, 2, 3, 3, 1, 2, 3],
    ]?;

    let vertices = GraphFrame::from_edges(edges.clone())?.out_degrees()?;

    let damping_factor = 0.85;
    let num_vertices: f64 = vertices.column(ID)?.len() as f64;

    let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges)?)
        .max_iterations(4)
        .with_vertex_column("rank")
        .initial_message(lit(1.0 / num_vertices))
        .send_messages(MessageReceiver::Dst, Pregel::src("rank") / Pregel::src("out_degree"))
        .aggregate_messages(Pregel::msg(None).sum())
        .v_prog(Pregel::msg(None) * lit(damping_factor) + lit((1.0 - damping_factor) / num_vertices))
        .build();

    Ok(println!("{}", pregel.run()?))
}