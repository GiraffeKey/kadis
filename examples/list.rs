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
		kadis.rpush("cats", Cat {
			name: "Herbert".into(),
			color: "orange".into(),
		}).await.unwrap();

		kadis.rpush("cats", Cat {
			name: "Ferb".into(),
			color: "black".into(),
		}).await.unwrap();

		kadis.lpush("cats", Cat {
			name: "Kirby".into(),
			color: "gray".into(),
		}).await.unwrap();

		let cat: Cat = kadis.lindex("cats", 1).await.unwrap();
		log::info!("{:?}", cat);
	});
}
