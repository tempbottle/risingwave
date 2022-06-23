// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// TODO(MrCroxx): Use offset and len / block idx and blocks.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct IndexKey {
    /// sst id
    sst: u64,
    /// block idx
    block: u32,
}

#[derive(Debug)]
pub struct IndexValue {
    /// cache file id
    fid: u32,
    /// slot idx
    slot: u32,
}
