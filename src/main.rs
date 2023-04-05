mod graph_frame;
mod pregel;

use polars::prelude::*;
use crate::graph_frame::GraphFrame;
use crate::pregel::{Pregel, PregelBuilder};

fn main() {
    let vertices = df! [
        "id" => [0, 1, 2, 3, 4],
    ].unwrap();

    let edges = df! [
        "src" => [0, 1, 2, 2, 3, 4, 4],
        "dst" => [1, 2, 4, 0, 4, 0, 2],
    ].unwrap();

    let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges).unwrap())
        .max_iterations(2)
        .initial_message(0)
        .send_messages(Pregel::<i32>::src("aux") + lit(1))
        .build();
    println!("{}", pregel.run());
}
