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

use subotai::node::Node;

use crate::util::{retrieve, store};

pub enum HashCmd {
	Del(String, Vec<String>),
	Exists,
	Get(String, Vec<String>),
	GetAll,
	IncrBy,
	Keys,
	Len,
	Set(String, Vec<String>, Vec<Vec<u8>>),
	SetNx,
	StrLen,
	Vals,
	Scan,
}

use HashCmd::*;

pub fn handle_hash_cmd(node: &mut Node, cmd: HashCmd) -> Option<Vec<Option<Vec<u8>>>> {
	match cmd {
		// Del(key, fields) => {
		// 	for field in fields {
		// 		let key = &format!("kh-{}-{}", key, field);
		// 		// kademlia.remove_record(&key);
		// 	}
		// 	None
		// },
		Get(key, fields) => {
			let mut values = Vec::new();

			for field in fields {
				let key = format!("kh-{}-{}", key, field);
				let value = retrieve(node, &key);
				values.push(value);
			}

			Some(values)
		},
		Set(key, fields, values) => {
			for i in 0..fields.len() {
				let key = format!("kh-{}-{}", key, fields[i]);
				store(node, &key, &values[i]);
			}
			None
		},
		_ => unimplemented!(),
	}
}
