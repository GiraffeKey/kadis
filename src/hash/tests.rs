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
use serde::{Deserialize, Serialize};

use crate::KadisBuilder;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Cat {
	name: String,
	color: String,
}

#[test]
fn hash() {
	let mut kadis = KadisBuilder::default().bootstraps(&["/ip4/0.0.0.0/tcp/5130"]).init().unwrap();

	task::block_on(async move {
		let res = kadis.hexists("cats", "herb").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), false);

		let cat = Cat {
			name: "Herbert".into(),
			color: "orange".into(),
		};

		let res = kadis.hset("cats", "herb", &cat).await;
		assert!(res.is_ok());

		let res = kadis.hexists("cats", "herb").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), true);
		
		let res = kadis.hget::<Cat>("cats", "herb").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), cat);

		let cat = Cat {
			name: "Herbie".into(),
			color: "orange".into(),
		};
		let res = kadis.hset("cats", "herb", &cat).await;
		assert!(res.is_ok());
		
		let res = kadis.hget::<Cat>("cats", "herb").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), cat);

		let res = kadis.hget::<Cat>("cats", "herbie").await;
		assert!(res.is_err());

		let res = kadis.hset_multiple("cats", &["herb", "ferb"], &[
			Cat {
				name: "Herbert".into(),
				color: "orange".into(),
			},
			Cat {
				name: "Ferb".into(),
				color: "black".into(),
			},
		]).await;
		assert!(res.is_ok());

		let res = kadis.hgetall::<Cat>("cats").await;
		assert!(res.is_ok());

		assert_eq!(
			res.unwrap().get("ferb").unwrap(),
			&Cat {
				name: "Ferb".into(),
				color: "black".into(),
			},
		);

		let res = kadis.hdel("cats", "ferb").await;
		assert!(res.is_ok());

		let res = kadis.hexists("cats", "ferb").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), false);

		let res = kadis.hset("nums", "n1", 6f32).await;
		assert!(res.is_ok());

		let res = kadis.hincr("nums", "n1", 2).await;
		assert!(res.is_ok());

		let res = kadis.hget::<f32>("nums", "n1").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), 8.0);

		let res = kadis.hset_multiple("nums", &["n3", "n2"], &[5f32, 12f32]).await;
		assert!(res.is_ok());

		let res = kadis.hkeys("nums").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), vec!["n1".to_string(), "n3".to_string(), "n2".to_string()]);

		let res = kadis.hlen("nums").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), 3);

		let res = kadis.hset_nx("nums", "n2", 14f32).await;
		assert!(res.is_ok());

		let res = kadis.hget::<f32>("nums", "n2").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), 12.0);

		let res = kadis.hvals::<f32>("nums").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), vec![8.0, 5.0, 12.0]);
	})
}
