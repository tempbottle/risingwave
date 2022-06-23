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

use std::collections::{BinaryHeap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use nix::sys::statfs::{statfs, FsType as NixFsType, EXT4_SUPER_MAGIC};
use parking_lot::RwLock;
use risingwave_common::cache::{LruCache, LruCacheEventListener};

use super::error::{Error, Result};
use super::file::CacheFileMeta;
use super::index::{IndexKey, IndexValue};

#[derive(Clone, Copy, Debug)]
pub enum FsType {
    Ext4,
    Xfs,
}

pub struct CacheFileManagerOptions {
    pub dir: String,
}

#[derive(PartialEq, Eq)]
struct HeapItem {
    /// cache file id
    fid: u64,
    /// remaining free slots
    free: u32,
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.free.cmp(&other.free)
    }
}

struct CacheFileManagerCore {
    metas: HashMap<u64, CacheFileMeta>,
    heap: BinaryHeap<HeapItem>,
}

pub struct CacheFileManager {
    dir: String,

    _fs_type: FsType,
    _fs_block_size: usize,

    core: RwLock<CacheFileManagerCore>,
}

impl CacheFileManager {
    pub fn open(options: CacheFileManagerOptions) -> Result<Self> {
        if !PathBuf::from(options.dir.as_str()).exists() {
            std::fs::create_dir_all(options.dir.as_str())?;
        }

        // Get file system type and block size by `statfs(2)`.
        let fs_stat = statfs(options.dir.as_str())?;
        let fs_type = match fs_stat.filesystem_type() {
            EXT4_SUPER_MAGIC => FsType::Ext4,
            // FYI: https://github.com/nix-rust/nix/issues/1742
            NixFsType(libc::XFS_SUPER_MAGIC) => FsType::Xfs,
            nix_fs_type => return Err(Error::UnsupportedFilesystem(nix_fs_type.0)),
        };
        let fs_block_size = fs_stat.block_size() as usize;

        Ok(Self {
            dir: options.dir,

            _fs_type: fs_type,
            _fs_block_size: fs_block_size,

            core: RwLock::new(CacheFileManagerCore {
                metas: HashMap::default(),
                heap: BinaryHeap::default(),
            }),
        })
    }

    pub async fn restore(&self, indices: &LruCache<IndexKey, IndexValue>) -> Result<()> {
        Ok(())
    }

    pub fn insert(&self) -> Result<IndexValue> {
        todo!()
    }

    pub fn erase(&self) -> Result<()> {
        todo!()
    }
}

impl LruCacheEventListener for CacheFileManager {
    type K = IndexKey;
    type T = IndexValue;

    fn on_evict(&self, _key: &Self::K, _value: &Self::T) {
        // TODO: Trigger invalidate cache slot.
    }

    fn on_erase(&self, _key: &Self::K, _value: &Self::T) {
        // TODO: Trigger invalidate cache slot.
    }
}

pub type CacheFileManagerRef = Arc<CacheFileManager>;

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<CacheFileManagerRef>();
    }
}
