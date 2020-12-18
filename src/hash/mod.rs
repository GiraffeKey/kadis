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

pub async fn handle_hash_cmd(node: &mut Node, cmd: HashCmd<'_>) -> HashResult {
	match cmd {
		Del(key, fields) => {
			for field in fields {
				let key = format!("kh-{}-{}", key, field);
				node.remove(&key);
			}
			
			let fields_key = format!("kh-fields-{}", key);
			let hash_fields = get_list!(node, fields_key, HashResult, Del, HDelError);

			let hash_fields = hash_fields.iter()
				.filter(|s| !fields.contains(&s.as_str()))
				.map(|s| s.into())
				.collect::<Vec<String>>();
			
			join_list!(node, fields_key, hash_fields, HashResult, Del, HDelError);

			HashResult::Del(Ok(()))
		},
		Exists(key, field) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = match node.get(&fields_key).await {
				Ok(fields) => split_list(fields),
				Err(err) => return match err {
					GetError::NotFound => HashResult::Exists(Ok(false)),
					GetError::QuorumFailed => HashResult::Exists(Err(HExistsError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					GetError::Timeout => HashResult::Exists(Err(HExistsError::Timeout {
						key: key.into(),
						field: field.into(),
					})),
				},
			};
			let exists = fields.contains(&field.into());
			HashResult::Exists(Ok(exists))
		},
		Get(key, field) => {
			let hash_key = format!("kh-{}-{}", key, field);
			match node.get(&hash_key).await {
				Ok(data) => HashResult::Get(Ok(data)),
				Err(err) => match err {
					GetError::NotFound => HashResult::Get(Err(HGetError::NotFound {
						key: key.into(),
						field: field.into(),
					})),
					GetError::QuorumFailed => HashResult::Get(Err(HGetError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					GetError::Timeout => HashResult::Get(Err(HGetError::Timeout {
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
						GetError::NotFound => HashResult::GetM(Err(HGetError::NotFound {
							key: key.into(),
							field: (*field).into(),
						})),
						GetError::QuorumFailed => HashResult::GetM(Err(HGetError::QuorumFailed {
							key: key.into(),
							field: (*field).into(),
						})),
						GetError::Timeout => HashResult::GetM(Err(HGetError::Timeout {
							key: key.into(),
							field: (*field).into(),
						})),
					},
				};
				values.push(value);
			}

			HashResult::GetM(Ok(values))
		},
		GetAll(key) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = get_list!(node, fields_key, HashResult, GetAll, HGetAllError);

			let mut values = HashMap::new();

			for field in fields {
				let hash_key = format!("kh-{}-{}", key, field);
				let value = match node.get(&hash_key).await {
					Ok(data) => data,
					Err(err) => return match err {
						GetError::NotFound => HashResult::GetAll(Err(HGetAllError::NotFound {
							key: key.into(),
							field,
						})),
						GetError::QuorumFailed => HashResult::GetAll(Err(HGetAllError::QuorumFailed {
							key: key.into(),
							field,
						})),
						GetError::Timeout => HashResult::GetAll(Err(HGetAllError::Timeout {
							key: key.into(),
							field,
						})),
					},
				};
				values.insert(field, value);
			}

			HashResult::GetAll(Ok(values))
		},
		Incr(key, field, inc) => {
			let hash_key = format!("kh-{}-{}", key, field);
			let value = match node.get(&hash_key).await {
				Ok(data) => data,
				Err(err) => return match err {
					GetError::NotFound => HashResult::Incr(Err(HIncrError::NotFound {
						key: key.into(),
						field: field.into(),
					})),
					GetError::QuorumFailed => HashResult::Incr(Err(HIncrError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					GetError::Timeout => HashResult::Incr(Err(HIncrError::Timeout {
						key: key.into(),
						field: field.into(),
					})),
				},
			};

			let value = match bincode::deserialize::<f32>(&value) {
				Ok(value) => value + inc,
				Err(_) => return HashResult::Incr(Err(HIncrError::NotANumber {
					key: key.into(),
					value,
				})),
			};
			let value = bincode::serialize(&value).unwrap();

			match node.put(&hash_key, value).await {
				Ok(()) => HashResult::Incr(Ok(())),
				Err(err) => return match err {
					PutError::QuorumFailed => HashResult::Incr(Err(HIncrError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					PutError::Timeout => HashResult::Incr(Err(HIncrError::Timeout {
						key: key.into(),
						field: field.into(),
					})),
				}
			}
		},
		Keys(key) => {
			let fields_key = format!("kh-fields-{}", key);
			let keys = get_list!(node, fields_key, HashResult, Keys, HKeysError);
			HashResult::Keys(Ok(keys))
		},
		Len(key) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = get_list!(node, fields_key, HashResult, Len, HLenError);
			HashResult::Len(Ok(fields.len()))
		},
		Set(key, field, value) => {
			let fields_key = format!("kh-fields-{}", key);
			let mut hash_fields = get_list_exists!(node, fields_key, HashResult, Set, HSetError);

			let hash_key = format!("kh-{}-{}", key, field);
			match node.put(&hash_key, value).await {
				Ok(()) => hash_fields.push(field.into()),
				Err(err) => return match err {
					PutError::QuorumFailed => HashResult::Set(Err(HSetError::QuorumFailed {
						key: key.into(),
						field: field.into(),
					})),
					PutError::Timeout => HashResult::Set(Err(HSetError::Timeout {
						key: key.into(),
						field: field.into(),
					})),
				}
			}

			join_list!(node, fields_key, hash_fields, HashResult, Set, HSetError);
		
			HashResult::Set(Ok(()))
		},
		SetM(key, fields, values) => {
			let fields_key = format!("kh-fields-{}", key);
			let mut hash_fields = get_list_exists!(node, fields_key, HashResult, SetM, HSetError);

			for i in 0..fields.len() {
				let field = fields[i];
				let value = values[i].clone();
				let hash_key = format!("kh-{}-{}", key, field);
				match node.put(&hash_key, value).await {
					Ok(()) => hash_fields.push(field.into()),
					Err(err) => return match err {
						PutError::QuorumFailed => HashResult::SetM(Err(HSetError::QuorumFailed {
							key: key.into(),
							field: field.into(),
						})),
						PutError::Timeout => HashResult::SetM(Err(HSetError::Timeout {
							key: key.into(),
							field: field.into(),
						})),
					}
				}
			}

			join_list!(node, fields_key, hash_fields, HashResult, SetM, HSetError);
		
			HashResult::SetM(Ok(()))
		},
		SetNx(key, field, value) => {
			let fields_key = format!("kh-fields-{}", key);
			let mut hash_fields = get_list_exists!(node, fields_key, HashResult, SetNx, HSetError);
			let exists = hash_fields.contains(&field.into());

			if !exists {
				let hash_key = format!("kh-{}-{}", key, field);
				match node.put(&hash_key, value).await {
					Ok(()) => hash_fields.push(field.into()),
					Err(err) => return match err {
						PutError::QuorumFailed => HashResult::SetNx(Err(HSetError::QuorumFailed {
							key: key.into(),
							field: field.into(),
						})),
						PutError::Timeout => HashResult::SetNx(Err(HSetError::Timeout {
							key: key.into(),
							field: field.into(),
						})),
					}
				}

				join_list!(node, fields_key, hash_fields, HashResult, SetNx, HSetError);
			}
		
			HashResult::SetNx(Ok(()))
		},
		Vals(key) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = get_list!(node, fields_key, HashResult, Vals, HValsError);

			let mut values = Vec::new();

			for field in fields {
				let hash_key = format!("kh-{}-{}", key, field);
				let value = match node.get(&hash_key).await {
					Ok(data) => data,
					Err(err) => return match err {
						GetError::NotFound => HashResult::Vals(Err(HValsError::NotFound {
							key: key.into(),
							field,
						})),
						GetError::QuorumFailed => HashResult::Vals(Err(HValsError::QuorumFailed {
							key: key.into(),
							field,
						})),
						GetError::Timeout => HashResult::Vals(Err(HValsError::Timeout {
							key: key.into(),
							field,
						})),
					},
				};
				values.push(value);
			}

			HashResult::Vals(Ok(values))
		},
	}
}
