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

use crate::node::{Node, GetError, PutError};

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

async fn get_fields(node: &mut Node, key: &str) -> Result<Vec<String>, GetError> {
	let fields = match node.get(&key).await {
		Ok(fields) => fields,
		Err(err) => return Err(err),
	};
	let fields = str::from_utf8(&fields).unwrap().split(",,").map(|s| s.into()).collect();
	Ok(fields)
}

pub async fn handle_hash_cmd(node: &mut Node, cmd: HashCmd<'_>) -> HashCmdResult {
	match cmd {
		Del(key, fields) => {
			for field in fields {
				let key = format!("kh-{}-{}", key, field);
				node.remove(&key);
			}
			
			let fields_key = format!("kh-fields-{}", key);
			let hash_fields = match get_fields(node, &fields_key).await {
				Ok(fields) => fields,
				Err(err) => return match err {
					GetError::NotFound => HashCmdResult::Del(Err(HDelError::NotFound { key: key.into() })),
					GetError::QuorumFailed => HashCmdResult::Del(Err(HDelError::QuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => HashCmdResult::Del(Err(HDelError::Timeout {
						key: key.into(),
					})),
				},
			};
			let hash_fields = hash_fields.iter()
				.filter(|s| !fields.contains(&s.as_str()))
				.map(|s| s.into())
				.collect::<Vec<String>>()
				.join(",,");
			let hash_fields = hash_fields.as_bytes().to_vec();

			match node.put(&fields_key, hash_fields).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => HashCmdResult::Del(Err(HDelError::QuorumFailed {
						key: key.into(),
					})),
					PutError::Timeout => HashCmdResult::Del(Err(HDelError::Timeout {
						key: key.into(),
					})),
				},
			}

			HashCmdResult::Del(Ok(()))
		},
		Exists(key, field) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = match get_fields(node, &fields_key).await {
				Ok(fields) => fields,
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
			let hash_key = format!("kh-fields-{}", key);
			let fields = match get_fields(node, &hash_key).await {
				Ok(fields) => fields,
				Err(err) => return match err {
					GetError::NotFound => HashCmdResult::GetAll(Err(HGetAllError::KeyNotFound {
						key: key.into(),
					})),
					GetError::QuorumFailed => HashCmdResult::GetAll(Err(HGetAllError::FieldsQuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => HashCmdResult::GetAll(Err(HGetAllError::FieldsTimeout {
						key: key.into(),
					})),
				},
			};

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
			let keys = match get_fields(node, &fields_key).await {
				Ok(fields) => fields,
				Err(err) => return match err {
					GetError::NotFound => HashCmdResult::Keys(Err(HKeysError::NotFound {
						key: key.into(),
					})),
					GetError::QuorumFailed => HashCmdResult::Keys(Err(HKeysError::QuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => HashCmdResult::Keys(Err(HKeysError::Timeout {
						key: key.into(),
					})),
				},
			};
			HashCmdResult::Keys(Ok(keys))
		},
		Len(key) => {
			let fields_key = format!("kh-fields-{}", key);
			let fields = match get_fields(node, &fields_key).await {
				Ok(fields) => fields,
				Err(err) => return match err {
					GetError::NotFound => HashCmdResult::Len(Err(HLenError::NotFound {
						key: key.into(),
					})),
					GetError::QuorumFailed => HashCmdResult::Len(Err(HLenError::QuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => HashCmdResult::Len(Err(HLenError::Timeout {
						key: key.into(),
					})),
				},
			};
			HashCmdResult::Len(Ok(fields.len()))
		},
		Set(key, field, value) => {
			let fields_key = format!("kh-fields-{}", key);
			let mut fields = match node.get(&fields_key).await {
				Ok(fields) => {
					str::from_utf8(&fields)
						.unwrap()
						.split(",,")
						.map(|s| s.into())
						.collect()
				},
				Err(err) => match err {
					GetError::NotFound => Vec::<String>::new(),
					GetError::QuorumFailed => return HashCmdResult::Set(Err(HSetError::FieldsQuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => return HashCmdResult::Set(Err(HSetError::FieldsTimeout {
						key: key.into(),
					})),
				},
			};

			let hash_key = format!("kh-{}-{}", key, field);
			match node.put(&hash_key, value).await {
				Ok(()) => fields.push(field.into()),
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

			let fields = fields.join(",,");
			let fields = fields.as_bytes().to_vec();

			match node.put(&fields_key, fields).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => HashCmdResult::Set(Err(HSetError::FieldsQuorumFailed {
						key: key.into(),
					})),
					PutError::Timeout => HashCmdResult::Set(Err(HSetError::FieldsTimeout {
						key: key.into(),
					})),
				}
			}
		
			HashCmdResult::Set(Ok(()))
		},
		SetM(key, fields, values) => {
			let fields_key = format!("kh-fields-{}", key);
			let mut hash_fields = match node.get(&fields_key).await {
				Ok(fields) => {
					str::from_utf8(&fields)
						.unwrap()
						.split(",,")
						.map(|s| s.into())
						.collect()
				},
				Err(err) => match err {
					GetError::NotFound => Vec::<String>::new(),
					GetError::QuorumFailed => return HashCmdResult::SetM(Err(HSetError::FieldsQuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => return HashCmdResult::SetM(Err(HSetError::FieldsTimeout {
						key: key.into(),
					})),
				},
			};

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

			let hash_fields = hash_fields.join(",,");
			let hash_fields = hash_fields.as_bytes().to_vec();

			match node.put(&fields_key, hash_fields).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => HashCmdResult::SetM(Err(HSetError::FieldsQuorumFailed {
						key: key.into(),
					})),
					PutError::Timeout => HashCmdResult::SetM(Err(HSetError::FieldsTimeout {
						key: key.into(),
					})),
				}
			}
		
			HashCmdResult::SetM(Ok(()))
		},
		SetNx(key, field, value) => {
			let fields_key = format!("kh-fields-{}", key);
			let mut fields = match node.get(&fields_key).await {
				Ok(fields) => {
					str::from_utf8(&fields)
						.unwrap()
						.split(",,")
						.map(|s| s.into())
						.collect()
				},
				Err(err) => match err {
					GetError::NotFound => Vec::<String>::new(),
					GetError::QuorumFailed => return HashCmdResult::SetNx(Err(HSetError::FieldsQuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => return HashCmdResult::SetNx(Err(HSetError::FieldsTimeout {
						key: key.into(),
					})),
				},
			};
			let exists = fields.contains(&field.into());

			if !exists {
				let hash_key = format!("kh-{}-{}", key, field);
				match node.put(&hash_key, value).await {
					Ok(()) => fields.push(field.into()),
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

				let fields = fields.join(",,");
				let fields = fields.as_bytes().to_vec();

				match node.put(&fields_key, fields).await {
					Ok(_) => (),
					Err(err) => return match err {
						PutError::QuorumFailed => HashCmdResult::SetNx(Err(HSetError::FieldsQuorumFailed {
							key: key.into(),
						})),
						PutError::Timeout => HashCmdResult::SetNx(Err(HSetError::FieldsTimeout {
							key: key.into(),
						})),
					}
				}
			}
		
			HashCmdResult::SetNx(Ok(()))
		},
		Vals(key) => {
			let hash_key = format!("kh-fields-{}", key);
			let fields = match get_fields(node, &hash_key).await {
				Ok(fields) => fields,
				Err(err) => return match err {
					GetError::NotFound => HashCmdResult::Vals(Err(HValsError::KeyNotFound {
						key: key.into(),
					})),
					GetError::QuorumFailed => HashCmdResult::Vals(Err(HValsError::FieldsQuorumFailed {
						key: key.into(),
					})),
					GetError::Timeout => HashCmdResult::Vals(Err(HValsError::FieldsTimeout {
						key: key.into(),
					})),
				},
			};

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
