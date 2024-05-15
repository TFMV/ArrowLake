use std::fmt;

#[derive(Debug)]
pub struct ArrowLakeError {
    details: String,
}

impl ArrowLakeError {
    pub fn new(msg: &str) -> ArrowLakeError {
        ArrowLakeError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for ArrowLakeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl std::error::Error for ArrowLakeError {
    fn description(&self) -> &str {
        &self.details
    }
}
