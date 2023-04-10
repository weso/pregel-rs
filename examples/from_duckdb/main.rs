use std::error::Error;
use pregel_rs::graph_frame::GraphFrame;

fn main() -> Result<(), Box<dyn Error>> {
    let dataframe = GraphFrame::from_duckdb("./examples/from_duckdb/example.duckdb")?;
    Ok(println!("{}", dataframe))
}
