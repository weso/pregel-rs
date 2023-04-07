mod graph_frame;
mod pregel;

use polars::prelude::*;
use crate::graph_frame::{DST, GraphFrame};
use crate::pregel::{MessageReceiver, Pregel, PregelBuilder};

fn main() {
    let edges = df! [
        "src" => [0, 1, 1, 2, 2, 3],
        "dst" => [1, 0, 3, 1, 3, 2],
    ].unwrap();

    let vertices = GraphFrame::from_edges(edges.clone())
        .unwrap()
        .out_degrees()
        .collect()
        .unwrap();

    let alpha = 0.15;
    let num_vertices: f64 = vertices
        .clone()
        .lazy()
        .select([count()])
        .collect()
        .unwrap()
        .pop()
        .unwrap()
        .sum()
        .unwrap();

    let graph = GraphFrame::new(vertices, edges.clone()).unwrap();

    let pregel = PregelBuilder::new(graph)
        .max_iterations(4)
        .with_vertex_column("rank")
        .initial_message(lit(1.0 / num_vertices))
        .send_messages(MessageReceiver::Dst, Pregel::src("rank") / Pregel::src("out_degree"))
        .aggregate_messages(Pregel::msg(None).sum())
        .v_prog(Pregel::msg(None) * Expr::from(1.0 - alpha) + Expr::from(alpha / num_vertices))
        .build();

    println!("{}", pregel.run().unwrap());
}