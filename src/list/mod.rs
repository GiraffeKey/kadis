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

use uuid::Uuid;

use crate::{get_list, get_list_exists, join_list};
use crate::node::{Node, GetError, PutError};
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

fn id() -> String {
	Uuid::new_v4().to_string()
}

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
		Insert(key, index, item, after) => {
			let items_key = format!("kl-items-{}", key);
			let mut list = get_list!(node, items_key, ListCmdResult, Insert, LInsertError);

			let index = if after {
				index + 1
			} else {
				index
			};

			if index > list.len() {
				return ListCmdResult::Insert(Err(LInsertError::OutOfBounds {
					key: key.into(),
					index,
					len: list.len(),
				}))
			}

			let id = id();
			let item_key = format!("kl-{}", id);

			match node.put(&item_key, item).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => ListCmdResult::Insert(Err(LInsertError::QuorumFailed {
						key: key.into(),
						index,
					})),
					PutError::Timeout => ListCmdResult::Insert(Err(LInsertError::Timeout {
						key: key.into(),
						index,
					})),
				}
			}

			list.insert(index, id);

			join_list!(node, items_key, list, ListCmdResult, Insert, LInsertError);

			ListCmdResult::Insert(Ok(()))
		},
		Push(key, item, right) => {
			let items_key = format!("kl-items-{}", key);
			let mut list = get_list_exists!(node, items_key, ListCmdResult, Push, LPushError);

			let id = id();
			let item_key = format!("kl-{}", id);

			match node.put(&item_key, item).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => ListCmdResult::Push(Err(LPushError::QuorumFailed {
						key: key.into(),
					})),
					PutError::Timeout => ListCmdResult::Push(Err(LPushError::Timeout {
						key: key.into(),
					})),
				}
			}

			if right {
				list.push(id);
			} else {
				list.insert(0, id);
			}

			join_list!(node, items_key, list, ListCmdResult, Push, LPushError);

			ListCmdResult::Push(Ok(()))
		},
		PushX(key, item, right) => {
			let items_key = format!("kl-items-{}", key);
			let mut list = match node.get(&items_key).await {
				Ok(list) => split_list(list),
				Err(err) => return match err {
					GetError::NotFound => ListCmdResult::PushX(Ok(())),
					GetError::QuorumFailed => ListCmdResult::PushX(Err(LPushError::KeyQuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => ListCmdResult::PushX(Err(LPushError::KeyTimeout {
						key: key.into(),
					})),
				},
			};

			let id = id();
			let item_key = format!("kl-{}", id);

			match node.put(&item_key, item).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => ListCmdResult::PushX(Err(LPushError::QuorumFailed {
						key: key.into(),
					})),
					PutError::Timeout => ListCmdResult::PushX(Err(LPushError::Timeout {
						key: key.into(),
					})),
				}
			}

			if right {
				list.push(id);
			} else {
				list.insert(0, id);
			}

			join_list!(node, items_key, list, ListCmdResult, PushX, LPushError);

			ListCmdResult::PushX(Ok(()))
		},
		_ => unimplemented!(),
	}
}
