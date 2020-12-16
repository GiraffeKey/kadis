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

use std::{
	collections::HashMap,
	str,
};

use anyhow::{anyhow, Result};

use crate::node::Node;
use crate::util::CmdResult;

pub enum HashCmd<'a> {
	Del(&'a str, &'a [&'a str]),
	Exists(&'a str, &'a str),
	Get(&'a str, &'a [&'a str]),
	GetAll(&'a str),
	Incr(&'a str, &'a str, f32),
	Keys(&'a str),
	Len(&'a str),
	Set(&'a str, &'a [&'a str], Vec<Vec<u8>>),
	SetNx(&'a str, &'a str, Vec<u8>),
	StrLen(&'a str, &'a str),
	Vals(&'a str),
}

use HashCmd::*;

async fn get_fields(node: &mut Node, key: &str) -> Result<Vec<String>> {
	let fields = match node.get(&key).await {
		CmdResult::Get(res) => match res {
			Ok(fields) => fields,
			Err(err) => return Err(err),
		},
		_ => unreachable!(),
	};
	let fields = str::from_utf8(&fields).unwrap().split(",,").map(|s| s.to_string()).collect();
	Ok(fields)
}

pub async fn handle_hash_cmd(node: &mut Node, cmd: HashCmd<'_>) -> Vec<CmdResult> {
	match cmd {
		Del(key, fields) => {
			for field in fields {
				let key = format!("kh-{}-{}", key, field);
				node.remove(&key);
			}
			
			let key = format!("kh-fields-{}", key);
			let hash_fields = match get_fields(node, &key).await {
				Ok(fields) => fields,
				Err(err) => return vec![CmdResult::Put(Err(err))],
			};
			let hash_fields = hash_fields.iter()
				.filter(|s| !fields.contains(&s.as_str()))
				.map(|s| s.to_string())
				.collect::<Vec<String>>()
				.join(",,");
			let hash_fields = hash_fields.as_bytes().to_vec();
			node.put(&key, hash_fields).await;

			vec![CmdResult::Put(Ok(()))]
		},
		Exists(key, field) => {
			let key = format!("kh-fields-{}", key);
			let fields = match get_fields(node, &key).await {
				Ok(fields) => fields,
				Err(err) => return if format!("{}", err) == "Not found" {
					vec![CmdResult::Cond(Ok(false))]
				} else {
					vec![CmdResult::Cond(Err(err))]
				},
			};
			let cond = fields.contains(&field.into());
			let value = CmdResult::Cond(Ok(cond));
			vec![value]
		},
		Get(key, fields) => {
			let mut values = Vec::new();

			for field in fields {
				let key = format!("kh-{}-{}", key, field);
				let value = node.get(&key).await;
				values.push(value);
			}

			values
		},
		GetAll(key) => {
			let hash_key = format!("kh-fields-{}", key);
			let fields = match get_fields(node, &hash_key).await {
				Ok(fields) => fields,
				Err(err) => return vec![CmdResult::GetAll(Err(err))],
			};

			let mut values = HashMap::new();

			for field in fields {
				let key = format!("kh-{}-{}", key, field);
				let value = match node.get(&key).await {
					CmdResult::Get(res) => res,
					_ => unreachable!(),
				};
				values.insert(field, value);
			}

			vec![CmdResult::GetAll(Ok(values))]
		},
		Incr(key, field, inc) => {
			let key = format!("kh-{}-{}", key, field);
			let value = match node.get(&key).await {
				CmdResult::Get(res) => match res {
					Ok(value) => value,
					Err(err) => return vec![CmdResult::Put(Err(err))],
				},
				_ => unreachable!(),
			};

			let value = match bincode::deserialize::<f32>(&value) {
				Ok(value) => value + inc,
				Err(err) => return vec![CmdResult::Put(Err(anyhow!(err)))],
			};

			let value = bincode::serialize(&value).unwrap();
			let res = node.put(&key, value).await;

			vec![res]
		},
		Set(key, fields, values) => {
			let mut results = Vec::new();

			let hash_key = format!("kh-fields-{}", key);
			let mut hash_fields = match node.get(&hash_key).await {
				CmdResult::Get(res) => match res {
					Ok(fields) => {
						str::from_utf8(&fields).unwrap().split(",,").map(|s| s.to_string()).collect()
					},
					Err(_) => Vec::new(),
				},
				_ => unreachable!(),
			};

			for i in 0..fields.len() {
				let key = format!("kh-{}-{}", key, fields[i]);
				let res = node.put(&key, values[i].clone()).await;
				match res {
					CmdResult::Put(Ok(())) => hash_fields.push(fields[i].into()),
					_ => (),
				}
				results.push(res);
			}

			let hash_fields = hash_fields.join(",,");
			let hash_fields = hash_fields.as_bytes().to_vec();
			node.put(&hash_key, hash_fields).await;
		
			results
		},
		_ => unimplemented!(),
	}
}

#[cfg(test)]
mod tests {
	use super::super::KadisBuilder;
	use async_std::task;
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
				res.unwrap().get("ferb").unwrap().as_ref().unwrap(),
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
		})
	}
}
