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

use crate::{get_list, get_list_exists, join_list};
use crate::node::{Node, GetError, PutError};
use crate::util::split_list;

mod error;
#[cfg(test)]
mod tests;

pub use error::*;

pub enum HashCmd<'a> {
	Del(&'a str, &'a [&'a str]),
	Exists(&'a str, &'a str),
	Get(&'a str, &'a str),
	GetM(&'a str, &'a [&'a str]),
	GetAll(&'a str),
	Incr(&'a str, &'a str, f32),
	Keys(&'a str),
	Len(&'a str),
	Set(&'a str, &'a str, Vec<u8>),
	SetM(&'a str, &'a [&'a str], Vec<Vec<u8>>),
	SetNx(&'a str, &'a str, Vec<u8>),
	Vals(&'a str),
}

use HashCmd::*;

pub async fn handle_hash_cmd(node: &mut Node, cmd: HashCmd<'_>) -> HashCmdResult {
	match cmd {
		Del(key, fields) => {
			for field in fields {
				let key = format!("kh-{}-{}", key, field);
				node.remove(&key);
			}
			
			let fields_key = format!("kh-fields-{}", key);
			let hash_fields = get_list!(node, fields_key, HashCmdResult, Del, HDelError);

			let hash_fields = hash_fields.iter()
				.filter(|s| !fields.contains(&s.as_str()))
				.map(|s| s.into())
				.collect::<Vec<String>>();
			
			join_list!(node, fields_key, hash_fields, HashCmdResult, Del, HDelError);

			HashCmdResult::Del(Ok(()))
		},
		Exists(key, field) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = match node.get(&fields_key).await {
				Ok(fields) => split_list(fields),
				Err(err) => return match err {
					GetError::NotFound => HashCmdResult::Exists(Ok(false)),
					GetError::QuorumFailed => HashCmdResult::Exists(Err(HExistsError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					GetError::Timeout => HashCmdResult::Exists(Err(HExistsError::Timeout {
						key: key.into(),
						field: field.into(),
					})),
				},
			};
			let exists = fields.contains(&field.into());
			HashCmdResult::Exists(Ok(exists))
		},
		Get(key, field) => {
			let hash_key = format!("kh-{}-{}", key, field);
			match node.get(&hash_key).await {
				Ok(data) => HashCmdResult::Get(Ok(data)),
				Err(err) => match err {
					GetError::NotFound => HashCmdResult::Get(Err(HGetError::NotFound {
						key: key.into(),
						field: field.into(),
					})),
					GetError::QuorumFailed => HashCmdResult::Get(Err(HGetError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					GetError::Timeout => HashCmdResult::Get(Err(HGetError::Timeout {
						key: key.into(),
						field: field.into(),
					})),
				},
			}
		},
		GetM(key, fields) => {
			let mut values = Vec::new();

			for field in fields {
				let hash_key = format!("kh-{}-{}", key, field);
				let value = match node.get(&hash_key).await {
					Ok(data) => data,
					Err(err) => return match err {
						GetError::NotFound => HashCmdResult::GetM(Err(HGetError::NotFound {
							key: key.into(),
							field: (*field).into(),
						})),
						GetError::QuorumFailed => HashCmdResult::GetM(Err(HGetError::QuorumFailed {
							key: key.into(),
							field: (*field).into(),
						})),
						GetError::Timeout => HashCmdResult::GetM(Err(HGetError::Timeout {
							key: key.into(),
							field: (*field).into(),
						})),
					},
				};
				values.push(value);
			}

			HashCmdResult::GetM(Ok(values))
		},
		GetAll(key) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = get_list!(node, fields_key, HashCmdResult, GetAll, HGetAllError);

			let mut values = HashMap::new();

			for field in fields {
				let hash_key = format!("kh-{}-{}", key, field);
				let value = match node.get(&hash_key).await {
					Ok(data) => data,
					Err(err) => return match err {
						GetError::NotFound => HashCmdResult::GetAll(Err(HGetAllError::NotFound {
							key: key.into(),
							field,
						})),
						GetError::QuorumFailed => HashCmdResult::GetAll(Err(HGetAllError::QuorumFailed {
							key: key.into(),
							field,
						})),
						GetError::Timeout => HashCmdResult::GetAll(Err(HGetAllError::Timeout {
							key: key.into(),
							field,
						})),
					},
				};
				values.insert(field, value);
			}

			HashCmdResult::GetAll(Ok(values))
		},
		Incr(key, field, inc) => {
			let hash_key = format!("kh-{}-{}", key, field);
			let value = match node.get(&hash_key).await {
				Ok(data) => data,
				Err(err) => return match err {
					GetError::NotFound => HashCmdResult::Incr(Err(HIncrError::NotFound {
						key: key.into(),
						field: field.into(),
					})),
					GetError::QuorumFailed => HashCmdResult::Incr(Err(HIncrError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					GetError::Timeout => HashCmdResult::Incr(Err(HIncrError::Timeout {
						key: key.into(),
						field: field.into(),
					})),
				},
			};

			let value = match bincode::deserialize::<f32>(&value) {
				Ok(value) => value + inc,
				Err(_) => return HashCmdResult::Incr(Err(HIncrError::NotANumber {
					key: key.into(),
					value,
				})),
			};
			let value = bincode::serialize(&value).unwrap();

			match node.put(&hash_key, value).await {
				Ok(()) => HashCmdResult::Incr(Ok(())),
				Err(err) => return match err {
					PutError::QuorumFailed => HashCmdResult::Incr(Err(HIncrError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					PutError::Timeout => HashCmdResult::Incr(Err(HIncrError::Timeout {
						key: key.into(),
						field: field.into(),
					})),
				}
			}
		},
		Keys(key) => {
			let fields_key = format!("kh-fields-{}", key);
			let keys = get_list!(node, fields_key, HashCmdResult, Keys, HKeysError);
			HashCmdResult::Keys(Ok(keys))
		},
		Len(key) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = get_list!(node, fields_key, HashCmdResult, Len, HLenError);
			HashCmdResult::Len(Ok(fields.len()))
		},
		Set(key, field, value) => {
			let fields_key = format!("kh-fields-{}", key);
			let mut hash_fields = get_list_exists!(node, fields_key, HashCmdResult, Set, HSetError);

			let hash_key = format!("kh-{}-{}", key, field);
			match node.put(&hash_key, value).await {
				Ok(()) => hash_fields.push(field.into()),
				Err(err) => return match err {
					PutError::QuorumFailed => HashCmdResult::Set(Err(HSetError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					PutError::Timeout => HashCmdResult::Set(Err(HSetError::Timeout {
						key: key.into(),
						field: field.into(),
					})),
				}
			}

			join_list!(node, fields_key, hash_fields, HashCmdResult, Set, HSetError);
		
			HashCmdResult::Set(Ok(()))
		},
		SetM(key, fields, values) => {
			let fields_key = format!("kh-fields-{}", key);
			let mut hash_fields = get_list_exists!(node, fields_key, HashCmdResult, SetM, HSetError);

			for i in 0..fields.len() {
				let field = fields[i];
				let value = values[i].clone();
				let hash_key = format!("kh-{}-{}", key, field);
				match node.put(&hash_key, value).await {
					Ok(()) => hash_fields.push(field.into()),
					Err(err) => return match err {
						PutError::QuorumFailed => HashCmdResult::SetM(Err(HSetError::QuorumFailed {
							key: key.into(),
							field: field.into(),
						})),
						PutError::Timeout => HashCmdResult::SetM(Err(HSetError::Timeout {
							key: key.into(),
							field: field.into(),
						})),
					}
				}
			}

			join_list!(node, fields_key, hash_fields, HashCmdResult, SetM, HSetError);
		
			HashCmdResult::SetM(Ok(()))
		},
		SetNx(key, field, value) => {
			let fields_key = format!("kh-fields-{}", key);
			let mut hash_fields = get_list_exists!(node, fields_key, HashCmdResult, SetNx, HSetError);
			let exists = hash_fields.contains(&field.into());

			if !exists {
				let hash_key = format!("kh-{}-{}", key, field);
				match node.put(&hash_key, value).await {
					Ok(()) => hash_fields.push(field.into()),
					Err(err) => return match err {
						PutError::QuorumFailed => HashCmdResult::SetNx(Err(HSetError::QuorumFailed {
							key: key.into(),
							field: field.into(),
						})),
						PutError::Timeout => HashCmdResult::SetNx(Err(HSetError::Timeout {
							key: key.into(),
							field: field.into(),
						})),
					}
				}

				join_list!(node, fields_key, hash_fields, HashCmdResult, SetNx, HSetError);
			}
		
			HashCmdResult::SetNx(Ok(()))
		},
		Vals(key) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = get_list!(node, fields_key, HashCmdResult, Vals, HValsError);

			let mut values = Vec::new();

			for field in fields {
				let hash_key = format!("kh-{}-{}", key, field);
				let value = match node.get(&hash_key).await {
					Ok(data) => data,
					Err(err) => return match err {
						GetError::NotFound => HashCmdResult::Vals(Err(HValsError::NotFound {
							key: key.into(),
							field,
						})),
						GetError::QuorumFailed => HashCmdResult::Vals(Err(HValsError::QuorumFailed {
							key: key.into(),
							field,
						})),
						GetError::Timeout => HashCmdResult::Vals(Err(HValsError::Timeout {
							key: key.into(),
							field,
						})),
					},
				};
				values.push(value);
			}

			HashCmdResult::Vals(Ok(values))
		},
	}
}
