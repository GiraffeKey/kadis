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

use crate::node::Node;
use crate::util::{EventResult, exists_result};

pub enum HashCmd<'a> {
	Del(&'a str, &'a [&'a str]),
	Exists(&'a str, &'a str),
	Get(&'a str, &'a [&'a str]),
	GetAll,
	Incr,
	Keys,
	Len,
	Set(&'a str, &'a [&'a str], Vec<Vec<u8>>),
	SetNx,
	StrLen,
	Vals,
	Scan,
}

use HashCmd::*;

pub async fn handle_hash_cmd(node: &mut Node, cmd: HashCmd<'_>) -> Option<Vec<EventResult>> {
	match cmd {
		Del(key, fields) => {
			for field in fields {
				let key = format!("kh-{}-{}", key, field);
				node.remove(&key);
			}
			None
		},
		Exists(key, field) => {
			let key = format!("kh-{}-{}", key, field);
			let value = node.get(&key).await;
			let value = exists_result(value);
			Some(vec![value])
		},
		Get(key, fields) => {
			let mut values = Vec::new();

			for field in fields {
				let key = format!("kh-{}-{}", key, field);
				let value = node.get(&key).await;
				values.push(value);
			}

			Some(values)
		},
		Set(key, fields, values) => {
			let mut results = Vec::new();

			for i in 0..fields.len() {
				let key = format!("kh-{}-{}", key, fields[i]);
				let res = node.put(&key, values[i].clone()).await;
				results.push(res);
			}
		
			Some(results)
		},
		_ => unimplemented!(),
	}
}

#[cfg(test)]
mod tests {
	use async_std::task;
	use super::super::KadisBuilder;
	use serde::{Deserialize, Serialize};

	#[derive(Debug, PartialEq, Deserialize, Serialize)]
	struct Cat {
		name: String,
		color: String,
	}

	#[test]
	fn hash() {
		let _ = KadisBuilder::default().port(5130).init().unwrap();

		let mut kadis = KadisBuilder::default().bootstraps(&["/ip4/0.0.0.0/tcp/5130"]).init().unwrap();

		task::block_on(async move {
			let res = kadis.hexists("cats", "herb").await;
			assert!(res.is_ok());
			assert_eq!(res.unwrap(), false);

			let cat = Cat {
				name: "Herbert".to_string(),
				color: "orange".to_string(),
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
				name: "Herbie".to_string(),
				color: "orange".to_string(),
			};
			let res = kadis.hset("cats", "herb", &cat).await;
			assert!(res.is_ok());
			
			let res = kadis.hget::<Cat>("cats", "herb").await;
			assert!(res.is_ok());
			assert_eq!(res.unwrap(), cat);

			let res = kadis.hget::<Cat>("cats", "herbie").await;
			assert!(res.is_err());
		})
	}
}
