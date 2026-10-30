//! Starts a service when a consumer group has unread messages, using one held
//! fetch.

use std::collections::HashMap;
use std::time::Duration;

use super::{CHECK_TIMEOUT, RECHECK, WorkSource};
use crate::config::{KafkaGroup, KafkaServer};
use crate::kafka::client::{Client, Partition};

pub(super) struct KafkaSource {
    pub(super) groups: Vec<KafkaGroup>,
    pub(super) server: KafkaServer,
}

impl WorkSource for KafkaSource {
    async fn wait_for_work(&mut self) -> Result<Option<String>, String> {
        let bootstrap = &self.server.bootstrap;
        let fetch = wait_for_unread(bootstrap, &self.groups, RECHECK);
        let unread = tokio::time::timeout(RECHECK + CHECK_TIMEOUT, fetch)
            .await
            .map_err(|_| format!("Kafka at {bootstrap} timed out"))??;
        Ok(unread.then(|| format!("unread Kafka messages at {bootstrap}")))
    }
}

/// Whether any group has unread messages, waiting up to `wait` for one to appear.
/// A fetch from the lowest committed offset returns data only if a group is behind.
async fn wait_for_unread(
    bootstrap: &str,
    groups: &[KafkaGroup],
    wait: Duration,
) -> Result<bool, String> {
    let mut client = Client::connect(bootstrap).await?;
    let mut topics: Vec<&String> = groups.iter().flat_map(|g| &g.topics).collect();
    topics.sort();
    topics.dedup();
    let partitions = client.partitions(&topics).await?;
    if partitions.is_empty() {
        tokio::time::sleep(wait).await;
        return Ok(false);
    }

    let mut offsets: HashMap<&Partition, i64> = HashMap::new();
    for group in groups {
        let own: Vec<&Partition> = partitions
            .iter()
            .filter(|(topic, _)| group.topics.contains(topic))
            .collect();
        let committed = client.committed(&group.consumer_group, &own).await?;
        for partition in own {
            // A partition the group never committed to is unread from the start.
            let offset = committed.get(partition).copied().unwrap_or(0);
            let lowest = offsets.entry(partition).or_insert(offset);
            *lowest = offset.min(*lowest);
        }
    }
    client.fetch(&offsets, wait).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::kafka::client::{ERROR_OFFSET_OUT_OF_RANGE, ERROR_UNKNOWN_TOPIC, topic_name};
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
    use kafka_protocol::messages::metadata_response::{
        MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };
    use kafka_protocol::messages::{
        ApiKey, FetchRequest, FetchResponse, MetadataResponse, OffsetFetchRequest,
        OffsetFetchResponse, RequestHeader, ResponseHeader,
    };
    use kafka_protocol::protocol::{Decodable, Encodable};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// One topic with one partition holding offsets `earliest..latest`.
    #[derive(Clone)]
    struct Fake {
        exists: bool,
        earliest: i64,
        latest: i64,
        committed: &'static [(&'static str, i64)],
    }

    fn encode<M: Encodable>(message: &M, version: i16) -> BytesMut {
        let mut out = BytesMut::new();
        message.encode(&mut out, version).unwrap();
        out
    }

    fn respond(fake: &Fake, api: ApiKey, version: i16, request: &mut Bytes) -> BytesMut {
        match api {
            ApiKey::Metadata => {
                let (error, partitions) = if fake.exists {
                    (0, vec![MetadataResponsePartition::default()])
                } else {
                    (ERROR_UNKNOWN_TOPIC, Vec::new())
                };
                let topic = MetadataResponseTopic::default()
                    .with_error_code(error)
                    .with_name(Some(topic_name("t")))
                    .with_partitions(partitions);
                encode(
                    &MetadataResponse::default().with_topics(vec![topic]),
                    version,
                )
            }
            ApiKey::OffsetFetch => {
                let group = OffsetFetchRequest::decode(request, version)
                    .unwrap()
                    .group_id;
                let offset = fake
                    .committed
                    .iter()
                    .find(|(name, _)| group.0.to_string() == *name)
                    .map_or(-1, |(_, offset)| *offset);
                let topic = OffsetFetchResponseTopic::default()
                    .with_name(topic_name("t"))
                    .with_partitions(vec![
                        OffsetFetchResponsePartition::default().with_committed_offset(offset),
                    ]);
                encode(
                    &OffsetFetchResponse::default().with_topics(vec![topic]),
                    version,
                )
            }
            ApiKey::Fetch => {
                let fetch = FetchRequest::decode(request, version).unwrap();
                let offset = fetch.topics[0].partitions[0].fetch_offset;
                let mut partition = PartitionData::default();
                if offset < fake.earliest {
                    partition = partition.with_error_code(ERROR_OFFSET_OUT_OF_RANGE);
                } else if offset < fake.latest {
                    partition = partition.with_records(Some(Bytes::from_static(b"batch")));
                }
                let topic = FetchableTopicResponse::default()
                    .with_topic(topic_name("t"))
                    .with_partitions(vec![partition]);
                encode(
                    &FetchResponse::default().with_responses(vec![topic]),
                    version,
                )
            }
            other => panic!("unexpected api {other:?}"),
        }
    }

    async fn fake_broker(fake: Fake) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            while let Ok(size) = stream.read_i32().await {
                let mut request = vec![0; size as usize];
                stream.read_exact(&mut request).await.unwrap();
                let mut request = Bytes::from(request);
                let api = ApiKey::try_from(i16::from_be_bytes([request[0], request[1]])).unwrap();
                let version = i16::from_be_bytes([request[2], request[3]]);
                let header =
                    RequestHeader::decode(&mut request, api.request_header_version(version))
                        .unwrap();
                let body = respond(&fake, api, version, &mut request);
                let head = encode(
                    &ResponseHeader::default().with_correlation_id(header.correlation_id),
                    0,
                );
                let size = (head.len() + body.len()) as i32;
                stream.write_all(&size.to_be_bytes()).await.unwrap();
                stream.write_all(&head).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        addr
    }

    fn group(name: &str) -> KafkaGroup {
        KafkaGroup {
            consumer_group: name.to_string(),
            server: "local".to_string(),
            topics: vec!["t".to_string()],
        }
    }

    #[tokio::test]
    async fn finds_unread_messages() {
        struct Case {
            name: &'static str,
            fake: Fake,
            groups: &'static [&'static str],
            expect: bool,
        }
        let fake = Fake {
            exists: true,
            earliest: 0,
            latest: 10,
            committed: &[("behind", 7), ("caught-up", 10)],
        };
        let cases = [
            Case {
                name: "behind",
                fake: fake.clone(),
                groups: &["behind"],
                expect: true,
            },
            Case {
                name: "caught up",
                fake: fake.clone(),
                groups: &["caught-up"],
                expect: false,
            },
            Case {
                name: "one group behind another",
                fake: fake.clone(),
                groups: &["caught-up", "behind"],
                expect: true,
            },
            Case {
                name: "never committed, with messages",
                fake: fake.clone(),
                groups: &["new"],
                expect: true,
            },
            Case {
                name: "never committed, empty",
                fake: Fake {
                    latest: 0,
                    ..fake.clone()
                },
                groups: &["new"],
                expect: false,
            },
            Case {
                name: "never committed, past retention",
                fake: Fake {
                    earliest: 5,
                    latest: 5,
                    ..fake.clone()
                },
                groups: &["new"],
                expect: true,
            },
            Case {
                name: "topic doesn't exist",
                fake: Fake {
                    exists: false,
                    ..fake
                },
                groups: &["new"],
                expect: false,
            },
        ];
        for case in cases {
            let addr = fake_broker(case.fake).await;
            let groups: Vec<KafkaGroup> = case.groups.iter().map(|name| group(name)).collect();
            let unread = wait_for_unread(&addr, &groups, Duration::from_millis(1))
                .await
                .unwrap();
            assert_eq!(unread, case.expect, "{}", case.name);
        }
    }

    #[tokio::test]
    async fn reports_an_unreachable_broker() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let error = wait_for_unread(&addr, &[group("g")], Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(error.contains("can't reach Kafka"), "{error}");
    }
}
