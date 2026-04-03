use std::pin::Pin;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use gitaly_protos::proto::ListBlobsResponse;
use gitaly_protos::proto::list_blobs_response::Blob as BlobChunk;
use prost::Message;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tokio_util::io::StreamReader;

#[derive(Debug)]
pub struct ResolvedBlob {
    pub oid: String,
    pub data: Vec<u8>,
}

struct InProgressBlob {
    oid: String,
    data: Vec<u8>,
}

impl InProgressBlob {
    fn into_resolved(self) -> ResolvedBlob {
        ResolvedBlob {
            oid: self.oid,
            data: self.data,
        }
    }
}

const MAX_FRAME_LENGTH: usize = 4 * 1024 * 1024; // 4 MiB
const MAX_BLOB_SIZE: usize = 1024 * 1024; // 1 MiB

type ChunkStream = Pin<Box<dyn Stream<Item = Result<BlobChunk, BlobDecodeError>> + Send>>;

pub struct BlobStream {
    chunks: ChunkStream,
    current: Option<InProgressBlob>,
}

impl BlobStream {
    pub fn new<S, E>(stream: Pin<Box<S>>) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + ?Sized + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let reader = StreamReader::new(stream.map(|r| r.map_err(std::io::Error::other)));

        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(MAX_FRAME_LENGTH);

        let chunks = FramedRead::new(reader, codec)
            .map(|frame| {
                let frame = frame.map_err(|e| BlobDecodeError(format!("frame error: {e}")))?;
                let resp = ListBlobsResponse::decode(frame)
                    .map_err(|e| BlobDecodeError(format!("protobuf decode error: {e}")))?;
                Ok(resp.blobs)
            })
            .flat_map(
                |result: Result<Vec<BlobChunk>, BlobDecodeError>| match result {
                    Ok(blobs) => futures::stream::iter(blobs.into_iter().map(Ok)).left_stream(),
                    Err(e) => futures::stream::once(async move { Err(e) }).right_stream(),
                },
            );

        Self {
            chunks: Box::pin(chunks),
            current: None,
        }
    }

    pub async fn drain(&mut self) -> (Vec<ResolvedBlob>, Option<BlobDecodeError>) {
        let mut blobs = Vec::new();
        loop {
            match self.next_blob().await {
                Ok(Some(blob)) => blobs.push(blob),
                Ok(None) => return (blobs, None),
                Err(e) => return (blobs, Some(e)),
            }
        }
    }

    pub async fn next_blob(&mut self) -> Result<Option<ResolvedBlob>, BlobDecodeError> {
        loop {
            match self.chunks.next().await {
                Some(Ok(chunk)) => {
                    if !chunk.oid.is_empty() {
                        let prev = self.current.replace(InProgressBlob {
                            oid: chunk.oid,
                            data: chunk.data,
                        });
                        if let Some(blob) = prev {
                            return Ok(Some(blob.into_resolved()));
                        }
                    } else if let Some(current) = &mut self.current {
                        current.data.extend_from_slice(&chunk.data);
                        if current.data.len() > MAX_BLOB_SIZE {
                            return Err(BlobDecodeError(format!(
                                "blob {} exceeds maximum size of {MAX_BLOB_SIZE} bytes",
                                current.oid
                            )));
                        }
                    }
                }
                Some(Err(e)) => return Err(e),
                None => return Ok(self.current.take().map(InProgressBlob::into_resolved)),
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct BlobDecodeError(String);

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_frame(response: &ListBlobsResponse) -> Vec<u8> {
        let frame = response.encode_to_vec();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(frame.len() as u32).to_be_bytes());
        buf.extend_from_slice(&frame);
        buf
    }

    fn blob_stream_from_bytes(data: Vec<u8>) -> BlobStream {
        let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            Box::pin(futures::stream::once(async move { Ok(Bytes::from(data)) }));
        BlobStream::new(stream)
    }

    fn blob_stream_from_chunks(chunks: Vec<Vec<u8>>) -> BlobStream {
        let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> = Box::pin(
            futures::stream::iter(chunks.into_iter().map(|c| Ok(Bytes::from(c)))),
        );
        BlobStream::new(stream)
    }

    async fn collect_blobs(data: Vec<u8>) -> Result<Vec<ResolvedBlob>, BlobDecodeError> {
        let mut decoder = blob_stream_from_bytes(data);
        let mut blobs = Vec::new();
        while let Some(blob) = decoder.next_blob().await? {
            blobs.push(blob);
        }
        Ok(blobs)
    }

    #[tokio::test]
    async fn decodes_single_blob_single_frame() {
        let response = ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: "abc123".into(),
                size: 5,
                data: b"hello".to_vec(),
                path: b"src/main.rs".to_vec(),
            }],
        };

        let blobs = collect_blobs(encode_frame(&response)).await.unwrap();

        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].oid, "abc123");
        assert_eq!(blobs[0].data, b"hello");
    }

    #[tokio::test]
    async fn decodes_multi_chunk_blob() {
        let chunk1 = ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: "abc123".into(),
                size: 10,
                data: b"hello".to_vec(),
                path: b"file.rs".to_vec(),
            }],
        };
        let chunk2 = ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: String::new(),
                size: 0,
                data: b"world".to_vec(),
                path: Vec::new(),
            }],
        };
        let mut data = encode_frame(&chunk1);
        data.extend_from_slice(&encode_frame(&chunk2));

        let blobs = collect_blobs(data).await.unwrap();

        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].oid, "abc123");
        assert_eq!(blobs[0].data, b"helloworld");
    }

    #[tokio::test]
    async fn decodes_multiple_blobs() {
        let response = ListBlobsResponse {
            blobs: vec![
                BlobChunk {
                    oid: "aaa".into(),
                    size: 3,
                    data: b"foo".to_vec(),
                    path: b"a.rs".to_vec(),
                },
                BlobChunk {
                    oid: "bbb".into(),
                    size: 3,
                    data: b"bar".to_vec(),
                    path: b"b.rs".to_vec(),
                },
            ],
        };

        let blobs = collect_blobs(encode_frame(&response)).await.unwrap();

        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].oid, "aaa");
        assert_eq!(blobs[0].data, b"foo");
        assert_eq!(blobs[1].oid, "bbb");
        assert_eq!(blobs[1].data, b"bar");
    }

    #[tokio::test]
    async fn empty_stream_returns_empty() {
        let blobs = collect_blobs(vec![]).await.unwrap();
        assert!(blobs.is_empty());
    }

    #[tokio::test]
    async fn truncated_frame_returns_error() {
        let response = ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: "abc".into(),
                size: 3,
                data: b"foo".to_vec(),
                path: Vec::new(),
            }],
        };
        let mut data = encode_frame(&response);
        data.truncate(data.len() - 2);

        let err = collect_blobs(data).await.unwrap_err();
        assert!(err.to_string().contains("frame error"));
    }

    #[tokio::test]
    async fn handles_frame_split_across_stream_chunks() {
        let response = ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: "abc123".into(),
                size: 5,
                data: b"hello".to_vec(),
                path: b"main.rs".to_vec(),
            }],
        };
        let data = encode_frame(&response);
        let mid = data.len() / 2;

        let mut decoder = blob_stream_from_chunks(vec![data[..mid].to_vec(), data[mid..].to_vec()]);
        let blob = decoder.next_blob().await.unwrap().unwrap();

        assert_eq!(blob.oid, "abc123");
        assert_eq!(blob.data, b"hello");
    }
}
