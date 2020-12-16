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
