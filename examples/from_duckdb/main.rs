use pregel_rs::graph_frame::GraphFrame;
use std::error::Error;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let connection = duckdb::Connection::open(Path::new("./examples/from_duckdb/example.duckdb"))?;
    let dataframe = GraphFrame::from_duckdb(connection)?;
    Ok(println!("{}", dataframe))
}
