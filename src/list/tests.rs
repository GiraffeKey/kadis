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
fn list() {
	let mut kadis = KadisBuilder::default().bootstraps(&["/ip4/0.0.0.0/tcp/5130"]).init().unwrap();

	task::block_on(async move {
		let cat = Cat {
			name: "Herbert".into(),
			color: "orange".into(),
		};
		let res = kadis.rpush("cats", &cat).await;
		assert!(res.is_ok());

		let res = kadis.rpush("cats", Cat {
			name: "Ferb".into(),
			color: "black".into(),
		}).await;
		assert!(res.is_ok());

		let res = kadis.lpush("cats", Cat {
			name: "Kirby".into(),
			color: "gray".into(),
		}).await;
		assert!(res.is_ok());

		let res = kadis.lindex::<Cat>("cats", 1).await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), cat);

		let cat = Cat {
			name: "Herbie".into(),
			color: "yellow".into(),
		};
		let res = kadis.linsert_before("cats", 1, &cat).await;
		assert!(res.is_ok());

		let res = kadis.lindex::<Cat>("cats", 1).await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), cat);

		let res = kadis.lrange::<Cat>("cats", 1, 3).await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), vec![
			Cat {
				name: "Herbie".into(),
				color: "yellow".into(),
			},
			Cat {
				name: "Herbert".into(),
				color: "orange".into(),
			},
			Cat {
				name: "Ferb".into(),
				color: "black".into(),
			},
		]);

		let res = kadis.lpop::<Cat>("cats").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), Cat {
			name: "Kirby".into(),
			color: "gray".into(),
		});

		let res = kadis.rpop::<Cat>("cats").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), Cat {
			name: "Ferb".into(),
			color: "black".into(),
		});

		let res = kadis.llen("cats").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), 2);

		let res = kadis.lcollect::<Cat>("cats").await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), vec![
			Cat {
				name: "Herbie".into(),
				color: "yellow".into(),
			},
			Cat {
				name: "Herbert".into(),
				color: "orange".into(),
			},
		]);

		let res = kadis.lpos("cats", Cat {
			name: "Herbert".into(),
			color: "orange".into(),
		}).await;
		assert!(res.is_ok());
		assert_eq!(res.unwrap(), Some(1));
	});
}
