//! Talks to one broker, which suits a single-broker dev cluster.

use std::collections::HashMap;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::{
    BrokerId, FetchRequest, GroupId, MetadataRequest, OffsetFetchRequest, RequestHeader,
    ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub(crate) const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;
pub(crate) const ERROR_UNKNOWN_TOPIC: i16 = 3;
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// A topic and partition number.
pub(crate) type Partition = (String, i32);

pub(crate) struct Client {
    bootstrap: String,
    stream: TcpStream,
    correlation_id: i32,
}

impl Client {
    pub(crate) async fn connect(bootstrap: &str) -> Result<Self, String> {
        let stream = TcpStream::connect(bootstrap)
            .await
            .map_err(|e| format!("can't reach Kafka at {bootstrap}: {e}"))?;
        Ok(Self {
            bootstrap: bootstrap.to_string(),
            stream,
            correlation_id: 0,
        })
    }

    async fn request<R: Request>(
        &mut self,
        request: &R,
        version: i16,
    ) -> Result<R::Response, String> {
        self.correlation_id += 1;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("don")));
        let mut frame = BytesMut::new();
        header
            .encode(&mut frame, R::header_version(version))
            .and_then(|()| request.encode(&mut frame, version))
            .map_err(|e| format!("can't encode Kafka request: {e}"))?;

        let bootstrap = self.bootstrap.clone();
        let io_error = |e: std::io::Error| format!("Kafka at {bootstrap}: {e}");
        let size = i32::try_from(frame.len()).map_err(|e| e.to_string())?;
        self.stream
            .write_all(&size.to_be_bytes())
            .await
            .map_err(io_error)?;
        self.stream.write_all(&frame).await.map_err(io_error)?;

        let size = self.stream.read_i32().await.map_err(io_error)?;
        let size = usize::try_from(size).map_err(|e| e.to_string())?;
        if size > MAX_RESPONSE_BYTES {
            return Err(format!("Kafka response of {size} bytes is too large"));
        }
        let mut response = vec![0; size];
        self.stream
            .read_exact(&mut response)
            .await
            .map_err(io_error)?;

        let mut response = Bytes::from(response);
        let header = ResponseHeader::decode(&mut response, R::Response::header_version(version))
            .map_err(|e| format!("bad Kafka response: {e}"))?;
        if header.correlation_id != self.correlation_id {
            return Err("Kafka response is out of order".to_string());
        }
        R::Response::decode(&mut response, version).map_err(|e| format!("bad Kafka response: {e}"))
    }

    /// Metadata v4, the first with `allow_auto_topic_creation`, so that asking
    /// about a topic can't create it. A topic that doesn't exist is left out.
    pub(crate) async fn partitions(
        &mut self,
        topics: &[&String],
    ) -> Result<Vec<Partition>, String> {
        let request = MetadataRequest::default()
            .with_topics(Some(
                topics
                    .iter()
                    .map(|topic| MetadataRequestTopic::default().with_name(Some(topic_name(topic))))
                    .collect(),
            ))
            .with_allow_auto_topic_creation(false);
        let mut partitions = Vec::new();
        for topic in self.request(&request, 4).await?.topics {
            let name = topic.name.map(|n| n.0.to_string()).unwrap_or_default();
            if topic.error_code != ERROR_UNKNOWN_TOPIC {
                check_error(topic.error_code, &name)?;
                for partition in topic.partitions {
                    check_error(partition.error_code, &name)?;
                    partitions.push((name.clone(), partition.partition_index));
                }
            }
        }
        Ok(partitions)
    }

    /// OffsetFetch v1. Leaves out a partition the group never committed to.
    pub(crate) async fn committed(
        &mut self,
        group: &str,
        partitions: &[&Partition],
    ) -> Result<HashMap<Partition, i64>, String> {
        let mut by_topic: HashMap<&str, Vec<i32>> = HashMap::new();
        for (topic, partition) in partitions.iter().copied() {
            by_topic.entry(topic).or_default().push(*partition);
        }
        let topics = by_topic.into_iter().map(|(topic, partitions)| {
            OffsetFetchRequestTopic::default()
                .with_name(topic_name(topic))
                .with_partition_indexes(partitions)
        });
        let request = OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_topics(Some(topics.collect()));

        let mut committed = HashMap::new();
        for topic in self.request(&request, 1).await?.topics {
            let name = topic.name.0.to_string();
            for partition in topic.partitions {
                check_error(partition.error_code, &name)?;
                if partition.committed_offset >= 0 {
                    committed.insert(
                        (name.clone(), partition.partition_index),
                        partition.committed_offset,
                    );
                }
            }
        }
        Ok(committed)
    }

    /// Fetch v4, held until data arrives or `wait` runs out, returning whether any
    /// did. It commits nothing. An offset past retention counts as data.
    pub(crate) async fn fetch(
        &mut self,
        offsets: &HashMap<&Partition, i64>,
        wait: Duration,
    ) -> Result<bool, String> {
        let mut by_topic: HashMap<&str, Vec<FetchPartition>> = HashMap::new();
        for ((topic, partition), offset) in offsets {
            by_topic.entry(topic).or_default().push(
                FetchPartition::default()
                    .with_partition(*partition)
                    .with_fetch_offset(*offset)
                    .with_partition_max_bytes(1),
            );
        }
        let topics = by_topic.into_iter().map(|(topic, partitions)| {
            FetchTopic::default()
                .with_topic(topic_name(topic))
                .with_partitions(partitions)
        });
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_wait_ms(i32::try_from(wait.as_millis()).map_err(|e| e.to_string())?)
            .with_min_bytes(1)
            .with_max_bytes(1)
            .with_topics(topics.collect());

        let mut data = false;
        for topic in self.request(&request, 4).await?.responses {
            for partition in topic.partitions {
                if partition.error_code != ERROR_OFFSET_OUT_OF_RANGE {
                    check_error(partition.error_code, &topic.topic.0)?;
                }
                data |= partition.error_code == ERROR_OFFSET_OUT_OF_RANGE
                    || partition.records.is_some_and(|records| !records.is_empty());
            }
        }
        Ok(data)
    }
}

pub(crate) fn topic_name(topic: &str) -> TopicName {
    TopicName(StrBytes::from_string(topic.to_string()))
}

fn check_error(error: i16, topic: &str) -> Result<(), String> {
    if error == 0 {
        Ok(())
    } else {
        Err(format!("Kafka error {error} for topic {topic}"))
    }
}
