use kadis::Kadis;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;

#[derive(Debug, Deserialize, Serialize)]
struct Cat {
	name: String,
	color: String,
}

fn main() {
	SimpleLogger::new().with_level(LevelFilter::Info).init().unwrap();

	let _kadis = Kadis::new(&[], 5130).unwrap();

	let mut kadis = Kadis::new(&["/ip4/0.0.0.0/tcp/5130"], 5131).unwrap();

	kadis.hset("cats", "herb", Cat {
		name: "Herbert".to_string(),
		color: "orange".to_string(),
	});
	
	let cat: Cat = kadis.hget("cats", "herb").unwrap();
	log::info!("{:?}", cat);
}
