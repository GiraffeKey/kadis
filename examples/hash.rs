use async_std::task;
use kadis::KadisBuilder;
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

	let _ = KadisBuilder::default().port(5130).init().unwrap();

	let mut kadis = KadisBuilder::default().bootstraps(&["/ip4/0.0.0.0/tcp/5130"]).init().unwrap();

	task::block_on(async move {
		kadis.hset("cats", "herb", Cat {
			name: "Herbert".to_string(),
			color: "orange".to_string(),
		}).await.unwrap();

		log::info!("{}", kadis.hexists("cats", "herb").await.unwrap());
		
		let cat: Cat = kadis.hget("cats", "herb").await.unwrap();
		log::info!("{:?}", cat);
	});
}
