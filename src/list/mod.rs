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
	Collect(&'a str),
	Index(&'a str, usize),
	Insert(&'a str, usize, Vec<u8>, bool),
	Len(&'a str),
	Pop(&'a str, bool),
	Pos(&'a str, Vec<u8>, i32),
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
		Collect(key) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListCmdResult, Collect, LCollectError);
			let mut items = Vec::new();

			for (index, id) in list.iter().enumerate() {
				let item_key = format!("kl-{}", id);
				match node.get(&item_key).await {
					Ok(data) => items.push(data),
					Err(err) => return match err {
						GetError::NotFound => ListCmdResult::Collect(Err(LCollectError::NotFound {
							key: key.into(),
							index,
						})),
						GetError::QuorumFailed => ListCmdResult::Collect(Err(LCollectError::QuorumFailed {
							key: key.into(),
							index,
						})),
						GetError::Timeout => ListCmdResult::Collect(Err(LCollectError::Timeout {
							key: key.into(),
							index,
						})),
					},
				};
			}

			ListCmdResult::Collect(Ok(items))
		},
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
		Len(key) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListCmdResult, Len, LLenError);
			ListCmdResult::Len(Ok(list.len()))
		},
		Pop(key, right) => {
			let items_key = format!("kl-items-{}", key);
			let mut list = get_list!(node, items_key, ListCmdResult, Pop, LPopError);

			if list.len() == 0 {
				return ListCmdResult::Pop(Err(LPopError::EmptyList {
					key: key.into(),
				}))
			}

			let index = if right {
				list.len() - 1
			} else {
				0
			};

			let id = list.remove(index);

			let item_key = format!("kl-{}", id);
			let item = match node.get(&item_key).await {
				Ok(data) => data,
				Err(err) => return match err {
					GetError::NotFound => ListCmdResult::Pop(Err(LPopError::NotFound {
						key: key.into(),
						index,
					})),
					GetError::QuorumFailed => ListCmdResult::Pop(Err(LPopError::QuorumFailed {
						key: key.into(),
						index,
					})),
					GetError::Timeout => ListCmdResult::Pop(Err(LPopError::Timeout {
						key: key.into(),
						index,
					})),
				},
			};

			node.remove(&item_key);

			join_list!(node, items_key, list, ListCmdResult, Pop, LPopError);

			ListCmdResult::Pop(Ok(item))
		},
		Pos(key, test_item, rank) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListCmdResult, Pos, LPosError);

			let list = if rank > 0 {
				list
			} else if rank < 0 {
				list.iter().rev().map(|s| s.into()).collect()
			} else {
				return ListCmdResult::Pos(Err(LPosError::RankZero {
					key: key.into(),
				}));
			};

			let mut found = 0;

			for (index, id) in list.iter().enumerate() {
				let item_key = format!("kl-{}", id);
				let item = match node.get(&item_key).await {
					Ok(data) => data,
					Err(err) => return match err {
						GetError::NotFound => ListCmdResult::Pos(Err(LPosError::NotFound {
							key: key.into(),
							index,
						})),
						GetError::QuorumFailed => ListCmdResult::Pos(Err(LPosError::QuorumFailed {
							key: key.into(),
							index,
						})),
						GetError::Timeout => ListCmdResult::Pos(Err(LPosError::Timeout {
							key: key.into(),
							index,
						})),
					},
				};
				if item == test_item {
					found += 1;
					if found == rank.abs() {
						return ListCmdResult::Pos(Ok(Some(index)));
					}
				}
			}

			ListCmdResult::Pos(Ok(None))
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
		Range(key, start, stop) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListCmdResult, Range, LRangeError);
			let mut items = Vec::new();

			if stop > list.len() {
				return ListCmdResult::Range(Err(LRangeError::OutOfBounds {
					key: key.into(),
					len: list.len(),
				}));
			}
			let list = &list[start..=stop];

			for (index, id) in list.iter().enumerate() {
				let item_key = format!("kl-{}", id);
				match node.get(&item_key).await {
					Ok(data) => items.push(data),
					Err(err) => return match err {
						GetError::NotFound => ListCmdResult::Range(Err(LRangeError::NotFound {
							key: key.into(),
							index,
						})),
						GetError::QuorumFailed => ListCmdResult::Range(Err(LRangeError::QuorumFailed {
							key: key.into(),
							index,
						})),
						GetError::Timeout => ListCmdResult::Range(Err(LRangeError::Timeout {
							key: key.into(),
							index,
						})),
					},
				};
			}

			ListCmdResult::Range(Ok(items))
		},
		_ => unimplemented!(),
	}
}
