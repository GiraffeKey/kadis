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
		// let fields = &["herb", "ferb"];
		// let cats = &[
		// 	Cat {
		// 		name: "Herbert".into(),
		// 		color: "orange".into(),
		// 	},
		// 	Cat {
		// 		name: "Ferb".into(),
		// 		color: "black".into(),
		// 	},
		// ];
		// kadis.hset_multiple("cats", fields, cats).await.unwrap();

		// log::info!("{}", kadis.hexists("cats", "herb").await.unwrap());
		
		// let cats = kadis.hgetall::<Cat>("cats").await.unwrap();
		// log::info!("{:?}", cats);

		// let cat: Cat = kadis.hget("cats", "ferb").await.unwrap();
		// log::info!("{:?}", cat);

		kadis.hset("nums", "n1", 6f32).await.unwrap();
		kadis.hincr("nums", "n1", 2).await.unwrap();
		let n1: f32 = kadis.hget("nums", "n1").await.unwrap();
		log::info!("{}", n1);
	});
}
