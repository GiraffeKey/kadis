// Copyright (C) 2020 GiraffeKey
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//

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
