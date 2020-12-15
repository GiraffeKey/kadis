use kadis::Kadis;
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;

#[derive(Debug, Deserialize, Serialize)]
struct Cat {
	name: String,
	color: String,
}

fn main() {
	SimpleLogger::new().init().unwrap();

	let mut kadis = Kadis::new().unwrap();

	kadis.hset("cats", "herb", Cat {
		name: "Herbert".to_string(),
		color: "orange".to_string(),
	});
	
	let cat: Cat = kadis.hget("cats", "herb").unwrap();
	println!("{:?}", cat);
}
