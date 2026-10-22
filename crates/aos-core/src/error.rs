use std::fmt;

/// Errors shared by the probe, its adapter, and native bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The context or entity has begun closing. It cannot be reopened.
    Closed,
    /// No message arrived within the receive deadline.
    Timeout,
    /// An argument is outside the supported probe contract.
    InvalidArgument(String),
    /// An interface definition or payload exceeded its resource admission budget.
    ResourceLimit(String),
    /// A supported JSON document did not satisfy its interface schema.
    Validation(String),
    /// The requested policy or capability is not implemented.
    Unsupported(String),
    /// The remote provider rejected or failed a request.
    Remote(String),
    /// A managed Node is not active, so no new business operation was admitted.
    Inactive(String),
    /// A bounded request/reply queue discarded this operation's data. The
    /// diagnostic identifies the queue; this does not authorize a retry or
    /// imply that the request was never executed.
    QueueOverflow(String),
    /// The query finished without a response before its deadline. Provider
    /// execution is unknown; absence of a reply is not proof of nonexecution.
    NoResponse(String),
    /// A backend operation failed; retain its diagnostic.
    Transport(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("context or entity is closed"),
            Self::Timeout => f.write_str("receive timed out"),
            Self::InvalidArgument(message) => write!(f, "invalid argument: {message}"),
            Self::ResourceLimit(message) => write!(f, "resource limit exceeded: {message}"),
            Self::Validation(message) => write!(f, "schema validation failed: {message}"),
            Self::Unsupported(message) => write!(f, "unsupported: {message}"),
            Self::Remote(message) => write!(f, "provider failed: {message}"),
            Self::Inactive(message) => write!(f, "inactive: {message}"),
            Self::QueueOverflow(message) => write!(f, "queue overflow: {message}"),
            Self::NoResponse(message) => write!(f, "no response: {message}"),
            Self::Transport(message) => write!(f, "transport failed: {message}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
