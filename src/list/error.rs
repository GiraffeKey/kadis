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

pub enum LIndexError {

}

pub enum ListCmdResult {
	Index(Result<Vec<u8>, LIndexError>),
	// Insert(Result<(), LInsertError>),
	// Len(Result<usize, LLenError>),
	// Pop(Result<Vec<u8>, LPopError>),
	// Pos(Result<usize, LPosError>),
	// Push(Result<(), LPushError>),
	// PushX(Result<(), LPushXError>),
	// Range(Result<Vec<Vec<u8>>, LRangeError>),
	// Rem(Result<(), LRemError>),
	// Set(Result<(), LSetError>),
	// Trim(Result<(), LTrimError>),
	// Move(Result<Vec<u8>, LMoveError>),
	// RPopLPush(Result<Vec<u8>, RPopLPushError>),
}
