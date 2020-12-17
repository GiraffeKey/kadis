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

use std::str;

use crate::get_list;
use crate::node::{Node, GetError};
use crate::util::split_list;

mod error;
#[cfg(test)]
mod tests;

pub use error::*;

pub enum ListCmd<'a> {
	Index(&'a str, usize),
	Insert(&'a str, usize, Vec<u8>, bool),
	Len(&'a str),
	Pop(&'a str, bool),
	Pos(&'a str, Vec<u8>, u32),
	Push(&'a str, Vec<u8>, bool),
	PushX(&'a str, Vec<u8>, bool),
	Range(&'a str, usize, usize),
	Rem(&'a str, usize),
	Set(&'a str, usize, Vec<u8>),
	Trim(&'a str, usize, usize),
	Move(&'a str, &'a str, bool),
	RPopLPush(&'a str, &'a str),
}

use ListCmd::*;

pub async fn handle_list_cmd(node: &mut Node, cmd: ListCmd<'_>) -> ListCmdResult {
	match cmd {
		Index(key, index) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListCmdResult, Index, LIndexError);

			let id = match list.get(index) {
				Some(id) => id,
				None => return ListCmdResult::Index(Err(LIndexError::NotFound {
					key: key.into(),
					index,
				})),
			};

			let item_key = format!("kl-{}", id);
			let item = match node.get(&item_key).await {
				Ok(data) => data,
				Err(err) => return match err {
					GetError::NotFound => ListCmdResult::Index(Err(LIndexError::NotFound {
						key: key.into(),
						index,
					})),
					GetError::QuorumFailed => ListCmdResult::Index(Err(LIndexError::QuorumFailed {
						key: key.into(),
						index,
					})),
					GetError::Timeout => ListCmdResult::Index(Err(LIndexError::Timeout {
						key: key.into(),
						index,
					})),
				},
			};

			ListCmdResult::Index(Ok(item))
		},
		Push(key, item, right) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListCmdResult, Push, LPushError);

			ListCmdResult::Push(Ok(()))
		},
		_ => unimplemented!(),
	}
}
