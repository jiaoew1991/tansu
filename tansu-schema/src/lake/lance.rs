// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{io::Cursor, marker::PhantomData};

use arrow::{ipc::writer::StreamWriter, record_batch::RecordBatch as ArrowRecordBatch};
use arrow_ipc_lance::reader::StreamReader;
use async_trait::async_trait;
use lance::{
    Error as LanceError,
    dataset::{Dataset, WriteParams},
    deps::{
        arrow_array::{RecordBatch as LanceRecordBatch, RecordBatchIterator},
        arrow_schema::ArrowError,
    },
};
use tracing::debug;
use url::Url;

use tansu_sans_io::{describe_configs_response::DescribeConfigsResult, record::inflated::Batch};

use crate::{
    AsArrow as _, Error, Registry, Result,
    lake::{LakeHouse, LakeHouseType},
};

use super::House;

#[derive(Clone, Debug, Default)]
pub struct Builder<L = PhantomData<Url>, R = PhantomData<Registry>> {
    location: L,
    schema_registry: R,
}

impl<L, R> Builder<L, R> {
    pub fn location(self, location: Url) -> Builder<Url, R> {
        Builder {
            location,
            schema_registry: self.schema_registry,
        }
    }

    pub fn schema_registry(self, schema_registry: Registry) -> Builder<L, Registry> {
        Builder {
            location: self.location,
            schema_registry,
        }
    }
}

impl Builder<Url, Registry> {
    pub fn build(self) -> Result<House> {
        Lance::try_from(self).map(House::Lance)
    }
}

#[derive(Clone, Debug)]
pub struct Lance {
    location: Url,
    schema_registry: Registry,
}

impl TryFrom<Builder<Url, Registry>> for Lance {
    type Error = Error;

    fn try_from(value: Builder<Url, Registry>) -> Result<Self, Self::Error> {
        Ok(Self {
            location: value.location,
            schema_registry: value.schema_registry,
        })
    }
}

impl Lance {
    fn dataset_uri(&self, topic: &str) -> String {
        let mut base = self.location.to_string();

        if !base.ends_with('/') {
            base.push('/');
        }

        base.push_str(topic);
        base
    }

    fn convert_batch(&self, batch: ArrowRecordBatch) -> Result<LanceRecordBatch> {
        let mut buffer = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut buffer, batch.schema().as_ref()).map_err(Error::from)?;
            writer.write(&batch).map_err(Error::from)?;
            writer.finish().map_err(Error::from)?;
        }

        let cursor = Cursor::new(buffer);
        let mut reader =
            StreamReader::try_new(cursor, None).map_err(|err| Error::Message(err.to_string()))?;

        reader
            .next()
            .transpose()
            .map_err(|err| Error::Message(err.to_string()))?
            .ok_or_else(|| Error::Message(String::from("unable to read lance batch")))
    }

    fn batch_reader(
        &self,
        batch: ArrowRecordBatch,
    ) -> Result<RecordBatchIterator<std::vec::IntoIter<Result<LanceRecordBatch, ArrowError>>>> {
        let batch = self.convert_batch(batch)?;
        let schema = batch.schema();
        Ok(RecordBatchIterator::new(
            vec![Ok::<_, ArrowError>(batch)].into_iter(),
            schema,
        ))
    }

    fn is_not_found(error: &LanceError) -> bool {
        matches!(
            error,
            LanceError::DatasetNotFound { .. } | LanceError::NotFound { .. }
        )
    }
}

#[async_trait]
impl LakeHouse for Lance {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        _offset: i64,
        inflated: &Batch,
        _config: DescribeConfigsResult,
    ) -> Result<()> {
        let record_batch = self
            .schema_registry
            .as_arrow(topic, partition, inflated, LakeHouseType::Parquet)
            .await?;

        let uri = self.dataset_uri(topic);
        debug!(%uri, "lance dataset");

        match Dataset::open(&uri).await {
            Ok(mut dataset) => {
                let reader = self.batch_reader(record_batch.clone())?;
                dataset.append(reader, None).await.map_err(Error::from)?;
            }

            Err(err) if Self::is_not_found(&err) => {
                let reader = self.batch_reader(record_batch)?;
                Dataset::write(reader, &uri, Some(WriteParams::default()))
                    .await
                    .map(|_| ())
                    .map_err(Error::from)?;
            }

            Err(err) => return Err(Error::from(err)),
        }

        Ok(())
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Parquet)
    }
}
