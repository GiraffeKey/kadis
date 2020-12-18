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

pub async fn handle_list_cmd(node: &mut Node, cmd: ListCmd<'_>) -> ListResult {
	match cmd {
		Collect(key) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListResult, Collect, LCollectError);
			let mut items = Vec::new();

			for (index, id) in list.iter().enumerate() {
				let item_key = format!("kl-{}", id);
				match node.get(&item_key).await {
					Ok(data) => items.push(data),
					Err(err) => return match err {
						GetError::NotFound => ListResult::Collect(Err(LCollectError::NotFound {
							key: key.into(),
							index,
						})),
						GetError::QuorumFailed => ListResult::Collect(Err(LCollectError::QuorumFailed {
							key: key.into(),
							index,
						})),
						GetError::Timeout => ListResult::Collect(Err(LCollectError::Timeout {
							key: key.into(),
							index,
						})),
					},
				};
			}

			ListResult::Collect(Ok(items))
		},
		Index(key, index) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListResult, Index, LIndexError);

			let id = match list.get(index) {
				Some(id) => id,
				None => return ListResult::Index(Err(LIndexError::NotFound {
					key: key.into(),
					index,
				})),
			};

			let item_key = format!("kl-{}", id);
			let item = match node.get(&item_key).await {
				Ok(data) => data,
				Err(err) => return match err {
					GetError::NotFound => ListResult::Index(Err(LIndexError::NotFound {
						key: key.into(),
						index,
					})),
					GetError::QuorumFailed => ListResult::Index(Err(LIndexError::QuorumFailed {
						key: key.into(),
						index,
					})),
					GetError::Timeout => ListResult::Index(Err(LIndexError::Timeout {
						key: key.into(),
						index,
					})),
				},
			};

			ListResult::Index(Ok(item))
		},
		Insert(key, index, item, after) => {
			let items_key = format!("kl-items-{}", key);
			let mut list = get_list!(node, items_key, ListResult, Insert, LInsertError);

			let index = if after {
				index + 1
			} else {
				index
			};

			if index > list.len() {
				return ListResult::Insert(Err(LInsertError::OutOfBounds {
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
					PutError::QuorumFailed => ListResult::Insert(Err(LInsertError::QuorumFailed {
						key: key.into(),
						index,
					})),
					PutError::Timeout => ListResult::Insert(Err(LInsertError::Timeout {
						key: key.into(),
						index,
					})),
				}
			}

			list.insert(index, id);

			join_list!(node, items_key, list, ListResult, Insert, LInsertError);

			ListResult::Insert(Ok(()))
		},
		Len(key) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListResult, Len, LLenError);
			ListResult::Len(Ok(list.len()))
		},
		Pop(key, right) => {
			let items_key = format!("kl-items-{}", key);
			let mut list = get_list!(node, items_key, ListResult, Pop, LPopError);

			if list.len() == 0 {
				return ListResult::Pop(Err(LPopError::EmptyList {
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
					GetError::NotFound => ListResult::Pop(Err(LPopError::NotFound {
						key: key.into(),
						index,
					})),
					GetError::QuorumFailed => ListResult::Pop(Err(LPopError::QuorumFailed {
						key: key.into(),
						index,
					})),
					GetError::Timeout => ListResult::Pop(Err(LPopError::Timeout {
						key: key.into(),
						index,
					})),
				},
			};

			node.remove(&item_key);

			join_list!(node, items_key, list, ListResult, Pop, LPopError);

			ListResult::Pop(Ok(item))
		},
		Pos(key, test_item, rank) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListResult, Pos, LPosError);

			let list = if rank > 0 {
				list
			} else if rank < 0 {
				list.iter().rev().map(|s| s.into()).collect()
			} else {
				return ListResult::Pos(Err(LPosError::RankZero {
					key: key.into(),
				}));
			};

			let mut found = 0;

			for (index, id) in list.iter().enumerate() {
				let item_key = format!("kl-{}", id);
				let item = match node.get(&item_key).await {
					Ok(data) => data,
					Err(err) => return match err {
						GetError::NotFound => ListResult::Pos(Err(LPosError::NotFound {
							key: key.into(),
							index,
						})),
						GetError::QuorumFailed => ListResult::Pos(Err(LPosError::QuorumFailed {
							key: key.into(),
							index,
						})),
						GetError::Timeout => ListResult::Pos(Err(LPosError::Timeout {
							key: key.into(),
							index,
						})),
					},
				};
				if item == test_item {
					found += 1;
					if found == rank.abs() {
						return ListResult::Pos(Ok(Some(index)));
					}
				}
			}

			ListResult::Pos(Ok(None))
		},
		Push(key, item, right) => {
			let items_key = format!("kl-items-{}", key);
			let mut list = get_list_exists!(node, items_key, ListResult, Push, LPushError);

			let id = id();
			let item_key = format!("kl-{}", id);

			match node.put(&item_key, item).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => ListResult::Push(Err(LPushError::QuorumFailed {
						key: key.into(),
					})),
					PutError::Timeout => ListResult::Push(Err(LPushError::Timeout {
						key: key.into(),
					})),
				}
			}

			if right {
				list.push(id);
			} else {
				list.insert(0, id);
			}

			join_list!(node, items_key, list, ListResult, Push, LPushError);

			ListResult::Push(Ok(()))
		},
		PushX(key, item, right) => {
			let items_key = format!("kl-items-{}", key);
			let mut list = match node.get(&items_key).await {
				Ok(list) => split_list(list),
				Err(err) => return match err {
					GetError::NotFound => ListResult::PushX(Ok(())),
					GetError::QuorumFailed => ListResult::PushX(Err(LPushError::KeyQuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => ListResult::PushX(Err(LPushError::KeyTimeout {
						key: key.into(),
					})),
				},
			};

			let id = id();
			let item_key = format!("kl-{}", id);

			match node.put(&item_key, item).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => ListResult::PushX(Err(LPushError::QuorumFailed {
						key: key.into(),
					})),
					PutError::Timeout => ListResult::PushX(Err(LPushError::Timeout {
						key: key.into(),
					})),
				}
			}

			if right {
				list.push(id);
			} else {
				list.insert(0, id);
			}

			join_list!(node, items_key, list, ListResult, PushX, LPushError);

			ListResult::PushX(Ok(()))
		},
		Range(key, start, stop) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListResult, Range, LRangeError);
			let mut items = Vec::new();

			if stop >= list.len() {
				return ListResult::Range(Err(LRangeError::OutOfBounds {
					key: key.into(),
					index: stop,
					len: list.len(),
				}));
			}
			let list = &list[start..=stop];

			for (index, id) in list.iter().enumerate() {
				let item_key = format!("kl-{}", id);
				match node.get(&item_key).await {
					Ok(data) => items.push(data),
					Err(err) => return match err {
						GetError::NotFound => ListResult::Range(Err(LRangeError::NotFound {
							key: key.into(),
							index,
						})),
						GetError::QuorumFailed => ListResult::Range(Err(LRangeError::QuorumFailed {
							key: key.into(),
							index,
						})),
						GetError::Timeout => ListResult::Range(Err(LRangeError::Timeout {
							key: key.into(),
							index,
						})),
					},
				};
			}

			ListResult::Range(Ok(items))
		},
		Rem(key, index) => {
			let items_key = format!("kl-items-{}", key);
			let mut list = get_list!(node, items_key, ListResult, Rem, LRemError);

			if index >= list.len() {
				return ListResult::Rem(Err(LRemError::OutOfBounds {
					key: key.into(),
					index,
					len: list.len(),
				}))
			}

			let id = list.remove(index);

			let item_key = format!("kl-{}", id);
			let item = match node.get(&item_key).await {
				Ok(data) => data,
				Err(err) => return match err {
					GetError::NotFound => ListResult::Rem(Err(LRemError::NotFound {
						key: key.into(),
						index,
					})),
					GetError::QuorumFailed => ListResult::Rem(Err(LRemError::QuorumFailed {
						key: key.into(),
						index,
					})),
					GetError::Timeout => ListResult::Rem(Err(LRemError::Timeout {
						key: key.into(),
						index,
					})),
				},
			};

			node.remove(&item_key);

			join_list!(node, items_key, list, ListResult, Rem, LRemError);

			ListResult::Rem(Ok(item))
		},
		Set(key, index, item) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListResult, Set, LSetError);

			if index >= list.len() {
				return ListResult::Set(Err(LSetError::OutOfBounds {
					key: key.into(),
					index,
					len: list.len(),
				}))
			}

			let id = &list[index];
			let item_key = format!("kl-{}", id);

			match node.put(&item_key, item).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => ListResult::Set(Err(LSetError::QuorumFailed {
						key: key.into(),
						index,
					})),
					PutError::Timeout => ListResult::Set(Err(LSetError::Timeout {
						key: key.into(),
						index,
					})),
				}
			}

			ListResult::Set(Ok(()))
		},
		Trim(key, start, stop) => {
			let items_key = format!("kl-items-{}", key);
			let list = get_list!(node, items_key, ListResult, Trim, LTrimError);

			if stop >= list.len() {
				return ListResult::Trim(Err(LTrimError::OutOfBounds {
					key: key.into(),
					index: stop,
					len: list.len(),
				}))
			}

			if start > 0 {
				for id in &list[0..start] {
					let item_key = format!("kl-{}", id);
					node.remove(&item_key);
				}
			}

			if stop < list.len() - 1 {
				for id in &list[stop + 1..list.len()] {
					let item_key = format!("kl-{}", id);
					node.remove(&item_key);
				}
			}

			let list = &list[start..=stop];

			join_list!(node, items_key, list, ListResult, Trim, LTrimError);

			ListResult::Trim(Ok(()))
		},
		Move(_key, _dest, _right) => unimplemented!(),
		RPopLPush(_key, _dest) => unimplemented!(),
	}
}
