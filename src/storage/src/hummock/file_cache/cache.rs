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

use std::sync::Arc;

use risingwave_common::cache::LruCache;

use super::cache_file_manager::{CacheFileManager, CacheFileManagerOptions, CacheFileManagerRef};
use super::error::Result;
use super::filter::Filter;
use super::index::{IndexKey, IndexValue};

pub struct FileCacheOptions {
    pub dir: String,
    pub capacity: usize,
    pub filters: Vec<Arc<dyn Filter>>,
}

#[derive(Clone)]
pub struct FileCache {
    _filters: Vec<Arc<dyn Filter>>,

    _indices: Arc<LruCache<IndexKey, IndexValue>>,

    _cache_file_manager: CacheFileManagerRef,
}

impl FileCache {
    pub async fn open(options: FileCacheOptions) -> Result<Self> {
        let cache_file_manager =
            CacheFileManager::open(CacheFileManagerOptions { dir: options.dir })?;
        let cache_file_manager = Arc::new(cache_file_manager);

        // TODO: Restore indices.
        let indices =
            LruCache::with_event_listeners(6, options.capacity, vec![cache_file_manager.clone()]);
        cache_file_manager.restore(&indices).await?;
        let indices = Arc::new(indices);

        Ok(Self {
            _filters: options.filters,

            _indices: indices,

            _cache_file_manager: cache_file_manager,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<FileCache>();
    }

    #[tokio::test]
    async fn test_file_cache_manager() {
        let ci: bool = std::env::var("RISINGWAVE_CI")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .expect("env $RISINGWAVE_CI must be 'true' or 'false'");

        let tempdir = if ci {
            tempfile::Builder::new().tempdir_in("/risingwave").unwrap()
        } else {
            tempfile::tempdir().unwrap()
        };

        let options = FileCacheOptions {
            dir: tempdir.path().to_str().unwrap().to_string(),
            capacity: 256 * 1024 * 1024, // 256 MiB
            filters: vec![],
        };
        let _manager = FileCache::open(options).await.unwrap();
    }
}
