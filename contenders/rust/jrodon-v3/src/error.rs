use crate::part_executor::PartJob;

/// Crate-wide error type covering S3 and Lambda SDK errors, I/O, serialization, and internal
/// conversion failures.
///
/// The blanket `From<SdkError<E, R>>` impl funnels all typed AWS SDK S3 operation errors into
/// [`Error::S3`] via `aws_sdk_s3::Error`. Lambda SDK errors are wrapped in [`Error::Lambda`].
/// [`Error::InvalidPartJobConversion`] is returned when a [`PartJob`] cannot be converted into
/// a [`DelegatedPartJob`] (e.g. it is a `Copy` job or contains no entries).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("s3 error: {0}")]
    S3(#[from] Box<aws_sdk_s3::Error>),
    #[error("lambda error: {0}")]
    Lambda(#[from] Box<aws_sdk_lambda::Error>),
    #[error("s3 bytestream error: {0}")]
    S3ByteStream(#[from] aws_sdk_s3::primitives::ByteStreamError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    SerdeBin(#[from] serde_json::Error),
    #[error("Task panicked: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("Could not parse CRC32: {0}")]
    Crc32Parse(#[from] base64::DecodeSliceError),
    #[error("{message}")]
    InvalidPartJobConversion {
        message: &'static str,
        part_job: PartJob,
    },
    #[error("{0}")]
    Custom(String),
}
/// Converts any typed SDK error into [`Error::S3`] by erasing the operation-specific type.
impl<E, R> From<aws_sdk_s3::error::SdkError<E, R>> for Error
where
    aws_sdk_s3::error::SdkError<E, R>: Into<aws_sdk_s3::Error>,
{
    fn from(err: aws_sdk_s3::error::SdkError<E, R>) -> Self {
        Error::S3(Box::new(err.into()))
    }
}
impl From<&str> for Error {
    fn from(err: &str) -> Self {
        Error::Custom(err.to_owned())
    }
}
impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::Custom(err)
    }
}
