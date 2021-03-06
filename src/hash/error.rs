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

use std::collections::HashMap;

#[derive(Debug)]
pub enum HDelError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
}

#[derive(Debug)]
pub enum HExistsError {
	QuorumFailed {
		key: String,
		field: String,
	},
	Timeout {
		key: String,
		field: String,
	},
}

#[derive(Debug)]
pub enum HGetError {
	NotFound {
		key: String,
		field: String,
	},
	QuorumFailed {
		key: String,
		field: String,
	},
	Timeout {
		key: String,
		field: String,
	},
}

#[derive(Debug)]
pub enum HGetAllError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	NotFound {
		key: String,
		field: String,
	},
	QuorumFailed {
		key: String,
		field: String,
	},
	Timeout {
		key: String,
		field: String,
	},
}

#[derive(Debug)]
pub enum HIncrError {
	NotFound {
		key: String,
		field: String,
	},
	QuorumFailed {
		key: String,
		field: String,
	},
	Timeout {
		key: String,
		field: String,
	},
	NotANumber {
		key: String,
		value: Vec<u8>,
	},
}

#[derive(Debug)]
pub enum HKeysError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
}

#[derive(Debug)]
pub enum HLenError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
}

#[derive(Debug)]
pub enum HSetError {
	QuorumFailed {
		key: String,
		field: String,
	},
	Timeout {
		key: String,
		field: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
}

#[derive(Debug)]
pub enum HValsError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	NotFound {
		key: String,
		field: String,
	},
	QuorumFailed {
		key: String,
		field: String,
	},
	Timeout {
		key: String,
		field: String,
	},
}

pub enum HashResult {
	Del(Result<(), HDelError>),
	Exists(Result<bool, HExistsError>),
	Get(Result<Vec<u8>, HGetError>),
	GetM(Result<Vec<Vec<u8>>, HGetError>),
	GetAll(Result<HashMap<String, Vec<u8>>, HGetAllError>),
	Incr(Result<(), HIncrError>),
	Keys(Result<Vec<String>, HKeysError>),
	Len(Result<usize, HLenError>),
	Set(Result<(), HSetError>),
	SetM(Result<(), HSetError>),
	SetNx(Result<(), HSetError>),
	Vals(Result<Vec<Vec<u8>>, HValsError>),
}
