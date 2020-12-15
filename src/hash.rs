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

use libp2p::kad::{
    Kademlia,
    record::{Key, store::MemoryStore},
};

pub enum HashCmd {
	Del(String, Vec<String>),
	Exists,
	Get,
	GetAll,
	IncrBy,
	Keys,
	Len,
	MGet,
	MSet,
	Set,
	SetNx,
	StrLen,
	Vals,
	Scan,
}

use HashCmd::*;

pub fn handle_hash_cmd(kademlia: &mut Kademlia<MemoryStore>, cmd: &HashCmd) {
	match cmd {
		Del(key, fields) => {
			for field in fields {
				let key = Key::new(&format!("kh-{}-{}", key, field));
				kademlia.remove_record(&key);
			}
		},
		_ => unimplemented!(),
	}
}
