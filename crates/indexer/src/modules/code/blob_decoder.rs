use std::pin::Pin;

use futures::{Stream, StreamExt, stream::Fuse};
use prost::Message;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tokio_util::io::StreamReader;

#[derive(Debug)]
pub struct DecodedBlob {
    pub oid: String,
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ListBlobsResponse {
    #[prost(message, repeated, tag = "1")]
    blobs: Vec<BlobChunk>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct BlobChunk {
    #[prost(string, tag = "1")]
    oid: String,
    #[prost(int64, tag = "2")]
    #[allow(dead_code)]
    size: i64,
    #[prost(bytes = "vec", tag = "3")]
    data: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    #[allow(dead_code)]
    path: Vec<u8>,
}

type ChunkStream = Fuse<Pin<Box<dyn Stream<Item = Result<BlobChunk, BlobDecodeError>> + Send>>>;

pub struct BlobStream {
    chunks: ChunkStream,
    current: Option<DecodedBlob>,
}

impl BlobStream {
    pub fn from_byte_stream<S, E>(stream: Pin<Box<S>>) -> Self
    where
        S: Stream<Item = Result<bytes::Bytes, E>> + Send + ?Sized + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let io_stream = stream.map(|r| r.map_err(std::io::Error::other));
        let reader = StreamReader::new(io_stream);
        let frames = FramedRead::new(reader, LengthDelimitedCodec::new());

        let chunks: Pin<Box<dyn Stream<Item = Result<BlobChunk, BlobDecodeError>> + Send>> =
            Box::pin(frames.flat_map(|frame_result| {
                let items: Vec<Result<BlobChunk, BlobDecodeError>> = match frame_result {
                    Ok(frame) => match ListBlobsResponse::decode(frame) {
                        Ok(response) => response.blobs.into_iter().map(Ok).collect(),
                        Err(e) => vec![Err(BlobDecodeError(format!(
                            "failed to decode ListBlobsResponse: {e}"
                        )))],
                    },
                    Err(e) => vec![Err(BlobDecodeError(format!("frame error: {e}")))],
                };
                futures::stream::iter(items)
            }));

        Self {
            chunks: chunks.fuse(),
            current: None,
        }
    }

    pub async fn next_blob(&mut self) -> Result<Option<DecodedBlob>, BlobDecodeError> {
        loop {
            match self.chunks.next().await {
                Some(Ok(chunk)) => {
                    if !chunk.oid.is_empty() {
                        let new_blob = DecodedBlob {
                            oid: chunk.oid,
                            data: chunk.data,
                        };

                        if let Some(completed) = self.current.replace(new_blob) {
                            return Ok(Some(completed));
                        }
                    } else if let Some(current) = &mut self.current {
                        current.data.extend_from_slice(&chunk.data);
                    }
                }
                Some(Err(e)) => return Err(e),
                None => return Ok(self.current.take()),
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
        let stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>> =
            Box::pin(futures::stream::once(async move {
                Ok(bytes::Bytes::from(data))
            }));
        BlobStream::from_byte_stream(stream)
    }

    fn blob_stream_from_chunks(chunks: Vec<Vec<u8>>) -> BlobStream {
        let stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>> =
            Box::pin(futures::stream::iter(
                chunks.into_iter().map(|c| Ok(bytes::Bytes::from(c))),
            ));
        BlobStream::from_byte_stream(stream)
    }

    async fn collect_blobs(data: Vec<u8>) -> Result<Vec<DecodedBlob>, BlobDecodeError> {
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
