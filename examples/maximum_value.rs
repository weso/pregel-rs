use polars::prelude::*;
use graph_rs::graph_frame::GraphFrame;
use graph_rs::pregel::{MessageReceiver, Pregel, PregelBuilder};

fn main() { // TODO: remove unwraps and clones :(
    let vertices = df![
        "id" => [0, 1, 2, 3],
        "value" => [3, 6, 2, 1],
    ].unwrap();

    let edges = df![
        "src" => [0, 1, 1, 2, 2, 3],
        "dst" => [1, 0, 3, 1, 3, 2],
    ].unwrap();

    let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges).unwrap())
        .max_iterations(4)
        .with_vertex_column("maximum_value")
        .initial_message(col("value"))
        .send_messages(MessageReceiver::Dst, Pregel::src("maximum_value"))
        .aggregate_messages(Pregel::msg(None).max())
        .v_prog(max_exprs([col("maximum_value"), Pregel::msg(None)]))
        .build();

    println!("{}", pregel.run().unwrap());
}