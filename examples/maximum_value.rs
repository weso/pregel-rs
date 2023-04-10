use std::error::Error;
use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::{MessageReceiver, Pregel, PregelBuilder};
use pregel_rs::pregel::ColumnIdentifier::{Custom, Dst, Id, Src};

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
        .send_messages(MessageReceiver::Dst, Pregel::src(Custom("max_value".to_owned())))
        .aggregate_messages(Pregel::msg(None).max())
        .v_prog(max_exprs([col(Custom("max_value".to_owned()).as_ref()), Pregel::msg(None)]))
        .build();

    Ok(println!("{}", pregel.run()?))
}