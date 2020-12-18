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

#[derive(Debug)]
pub enum LCollectError {
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
		index: usize,
	},
	QuorumFailed {
		key: String,
		index: usize,
	},
	Timeout {
		key: String,
		index: usize,
	},
}

#[derive(Debug)]
pub enum LIndexError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	OutOfBounds {
		key: String,
		index: isize,
		len: usize,
	},
	NotFound {
		key: String,
		index: isize,
	},
	QuorumFailed {
		key: String,
		index: isize,
	},
	Timeout {
		key: String,
		index: isize,
	},
}

#[derive(Debug)]
pub enum LInsertError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	OutOfBounds {
		key: String,
		index: isize,
		len: usize,
	},
	QuorumFailed {
		key: String,
		index: isize,
	},
	Timeout {
		key: String,
		index: isize,
	},
}

#[derive(Debug)]
pub enum LPopError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	EmptyList {
		key: String,
	},
	NotFound {
		key: String,
		index: usize,
	},
	QuorumFailed {
		key: String,
		index: usize,
	},
	Timeout {
		key: String,
		index: usize,
	},
}

#[derive(Debug)]
pub enum LPosError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	RankZero {
		key: String,
	},
	NotFound {
		key: String,
		index: usize,
	},
	QuorumFailed {
		key: String,
		index: usize,
	},
	Timeout {
		key: String,
		index: usize,
	},
}

#[derive(Debug)]
pub enum LLenError {
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
pub enum LPushError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	QuorumFailed {
		key: String,
	},
	Timeout {
		key: String,
	},
}

#[derive(Debug)]
pub enum LRangeError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	OutOfBounds {
		key: String,
		index: isize,
		len: usize,
	},
	NotFound {
		key: String,
		index: usize,
	},
	QuorumFailed {
		key: String,
		index: usize,
	},
	Timeout {
		key: String,
		index: usize,
	},
}

#[derive(Debug)]
pub enum LRemError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	OutOfBounds {
		key: String,
		index: isize,
		len: usize,
	},
	NotFound {
		key: String,
		index: isize,
	},
	QuorumFailed {
		key: String,
		index: isize,
	},
	Timeout {
		key: String,
		index: isize,
	},
}

#[derive(Debug)]
pub enum LSetError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	OutOfBounds {
		key: String,
		index: isize,
		len: usize,
	},
	NotFound {
		key: String,
		index: isize,
	},
	QuorumFailed {
		key: String,
		index: isize,
	},
	Timeout {
		key: String,
		index: isize,
	},
}

#[derive(Debug)]
pub enum LTrimError {
	KeyNotFound {
		key: String,
	},
	KeyQuorumFailed {
		key: String,
	},
	KeyTimeout {
		key: String,
	},
	OutOfBounds {
		key: String,
		index: isize,
		len: usize,
	},
	QuorumFailed {
		key: String,
		index: isize,
	},
	Timeout {
		key: String,
		index: isize,
	},
}

pub enum ListResult {
	Collect(Result<Vec<Vec<u8>>, LCollectError>),
	Index(Result<Vec<u8>, LIndexError>),
	Insert(Result<(), LInsertError>),
	Len(Result<usize, LLenError>),
	Pop(Result<Vec<u8>, LPopError>),
	Pos(Result<Option<usize>, LPosError>),
	Push(Result<(), LPushError>),
	PushX(Result<(), LPushError>),
	Range(Result<Vec<Vec<u8>>, LRangeError>),
	Rem(Result<Vec<u8>, LRemError>),
	Set(Result<(), LSetError>),
	Trim(Result<(), LTrimError>),
}
