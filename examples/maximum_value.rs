use std::error::Error;
use polars::prelude::*;
use graph_rs::graph_frame::GraphFrame;
use graph_rs::pregel::{MessageReceiver, Pregel, PregelBuilder};

fn main() -> Result<(), Box<dyn Error>> {
    let edges = df![
        "src" => [0, 1, 1, 2, 2, 3],
        "dst" => [1, 0, 3, 1, 3, 2],
    ]?;

    let vertices = df![
        "id" => [0, 1, 2, 3],
        "value" => [3, 6, 2, 1],
    ]?;

    let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges)?)
        .max_iterations(4)
        .with_vertex_column("maximum_value")
        .initial_message(col("value"))
        .send_messages(MessageReceiver::Dst, Pregel::src("maximum_value"))
        .aggregate_messages(Pregel::msg(None).max())
        .v_prog(max_exprs([col("maximum_value"), Pregel::msg(None)]))
        .build();

    Ok(println!("{}", pregel.run()?))
}