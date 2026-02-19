#[derive(thiserror::Error, Debug)]
pub enum NostrError {
    #[error("Key derivation error: {0}")]
    KeyDerivationError(String),
    #[error("Zap receipt creation error: {0}")]
    ZapReceiptCreationError(String),
}
