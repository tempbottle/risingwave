// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};

use crate::error::StorageResult;
use crate::store::*;

/// A panic state store. If a workload is fully in-memory, we can use this state store to
/// ensure that no data is stored in the state store and no serialization will happen.
#[derive(Clone, Default)]
pub struct PanicStateStore;

impl StateStoreGet for PanicStateStore {
    fn on_key_value<O: Send + 'static>(
        &self,
        _key: TableKey<Bytes>,
        _read_options: ReadOptions,
        _on_key_value_fn: impl KeyValueFn<O>,
    ) -> impl StorageFuture<'_, Option<O>> {
        async { panic!("should not read from PanicStateStore") }
    }
}

impl StateStoreRead for PanicStateStore {
    type Iter = PanicStateStoreIter<StateStoreKeyedRow>;
    type RevIter = PanicStateStoreIter<StateStoreKeyedRow>;

    async fn iter(
        &self,
        _key_range: TableKeyRange,

        _read_options: ReadOptions,
    ) -> StorageResult<Self::Iter> {
        panic!("should not read from the state store!");
    }

    async fn rev_iter(
        &self,
        _key_range: TableKeyRange,

        _read_options: ReadOptions,
    ) -> StorageResult<Self::RevIter> {
        panic!("should not read from the state store!");
    }
}

impl StateStoreReadLog for PanicStateStore {
    type ChangeLogIter = PanicStateStoreIter<StateStoreReadLogItem>;

    async fn next_epoch(&self, _epoch: u64, _options: NextEpochOptions) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn iter_log(
        &self,
        _epoch_range: (u64, u64),
        _key_range: TableKeyRange,
        _options: ReadLogOptions,
    ) -> StorageResult<Self::ChangeLogIter> {
        unimplemented!()
    }
}

impl LocalStateStore for PanicStateStore {
    type FlushedSnapshotReader = PanicStateStore;
    type Iter<'a> = PanicStateStoreIter<StateStoreKeyedRow>;
    type RevIter<'a> = PanicStateStoreIter<StateStoreKeyedRow>;

    async fn iter(
        &self,
        _key_range: TableKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<Self::Iter<'_>> {
        panic!("should not operate on the panic state store!");
    }

    async fn rev_iter(
        &self,
        _key_range: TableKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<Self::RevIter<'_>> {
        panic!("should not operate on the panic state store!");
    }

    fn insert(
        &mut self,
        _key: TableKey<Bytes>,
        _new_val: Bytes,
        _old_val: Option<Bytes>,
    ) -> StorageResult<()> {
        panic!("should not operate on the panic state store!");
    }

    fn delete(&mut self, _key: TableKey<Bytes>, _old_val: Bytes) -> StorageResult<()> {
        panic!("should not operate on the panic state store!");
    }

    fn get_table_watermark(&self, _vnode: VirtualNode) -> Option<Bytes> {
        panic!("should not operate on the panic state store!");
    }

    fn new_flushed_snapshot_reader(&self) -> Self::FlushedSnapshotReader {
        panic!()
    }

    async fn update_vnode_bitmap(&mut self, _vnodes: Arc<Bitmap>) -> StorageResult<Arc<Bitmap>> {
        panic!("should not operate on the panic state store!");
    }
}

impl StateStoreWriteEpochControl for PanicStateStore {
    async fn flush(&mut self) -> StorageResult<usize> {
        panic!("should not operate on the panic state store!");
    }

    async fn init(&mut self, _epoch: InitOptions) -> StorageResult<()> {
        panic!("should not operate on the panic state store!");
    }

    fn seal_current_epoch(&mut self, _next_epoch: u64, _opts: SealCurrentEpochOptions) {
        panic!("should not operate on the panic state store!")
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        panic!("should not operate on the panic state store!");
    }
}

impl StateStoreWriteVector for PanicStateStore {
    fn insert(&mut self, _vec: Vector, _info: Bytes) -> StorageResult<()> {
        panic!()
    }
}

impl StateStoreReadVector for PanicStateStore {
    async fn nearest<O: Send + 'static>(
        &self,
        _vec: Vector,
        _options: VectorNearestOptions,
        _on_nearest_item_fn: impl OnNearestItemFn<O>,
    ) -> StorageResult<Vec<O>> {
        panic!()
    }
}

impl StateStore for PanicStateStore {
    type Local = Self;
    type ReadSnapshot = Self;
    type VectorWriter = PanicStateStore;

    async fn try_wait_epoch(
        &self,
        _epoch: HummockReadEpoch,
        _options: TryWaitEpochOptions,
    ) -> StorageResult<()> {
        panic!("should not wait epoch from the panic state store!");
    }

    async fn new_local(&self, _option: NewLocalOptions) -> Self::Local {
        panic!("should not call new local from the panic state store");
    }

    async fn new_read_snapshot(
        &self,
        _epoch: HummockReadEpoch,
        _options: NewReadSnapshotOptions,
    ) -> StorageResult<Self::ReadSnapshot> {
        panic!()
    }

    async fn new_vector_writer(&self, _options: NewVectorWriterOptions) -> Self::VectorWriter {
        panic!()
    }
}

pub struct PanicStateStoreIter<T: IterItem>(PhantomData<T>);

impl<T: IterItem> StateStoreIter<T> for PanicStateStoreIter<T> {
    async fn try_next(&mut self) -> StorageResult<Option<T::ItemRef<'_>>> {
        panic!("should not call next on panic state store iter")
    }
}
