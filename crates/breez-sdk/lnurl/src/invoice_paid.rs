use bitcoin::hashes::{Hash, sha256};
use tokio::sync::watch;
use tracing::{debug, error};

use crate::repository::{Invoice, LnurlRepository, LnurlRepositoryError, NewlyPaid};
use crate::time::now_millis;

#[derive(Debug, thiserror::Error)]
pub enum HandleInvoicePaidError {
    #[error("invalid preimage: {0}")]
    InvalidPreimage(String),
    #[error(transparent)]
    Repository(#[from] LnurlRepositoryError),
}

/// Verify that the SHA-256 hash of the preimage matches the expected payment hash.
/// Both values are hex-encoded strings.
fn verify_preimage(payment_hash: &str, preimage: &str) -> Result<(), HandleInvoicePaidError> {
    let preimage_bytes = hex::decode(preimage).map_err(|e| {
        HandleInvoicePaidError::InvalidPreimage(format!("could not hex-decode preimage: {e}"))
    })?;
    let computed_hash = sha256::Hash::hash(&preimage_bytes).to_string();
    if computed_hash != payment_hash {
        return Err(HandleInvoicePaidError::InvalidPreimage(
            "preimage does not match payment hash".to_string(),
        ));
    }
    Ok(())
}

/// Handle an invoice being paid by storing the preimage and queueing for background processing.
pub async fn handle_invoice_paid<DB>(
    db: &DB,
    payment_hash: &str,
    preimage: &str,
    trigger: &watch::Sender<()>,
) -> Result<(), HandleInvoicePaidError>
where
    DB: LnurlRepository + Clone + Send + Sync + 'static,
{
    verify_preimage(payment_hash, preimage)?;

    let now = now_millis();

    // Get the existing invoice
    let Some(mut invoice) = db.get_invoice_by_payment_hash(payment_hash).await? else {
        debug!(
            "Invoice not found for payment hash {}, cannot mark as paid",
            payment_hash
        );
        return Ok(());
    };

    // Check if already paid
    if invoice.preimage.is_some() {
        debug!("Invoice {} already has preimage, skipping", payment_hash);
        return Ok(());
    }

    // Store the preimage
    invoice.preimage = Some(preimage.to_string());
    invoice.updated_at = now;
    db.upsert_invoice(&invoice).await?;
    debug!("Stored preimage for invoice {}", payment_hash);

    // Queue for background processing (zap receipt publishing)
    let newly_paid = NewlyPaid {
        payment_hash: payment_hash.to_string(),
        created_at: now,
        retry_count: 0,
        next_retry_at: now, // Process immediately
    };
    db.insert_newly_paid(&newly_paid).await?;
    debug!("Queued invoice {} for background processing", payment_hash);

    // Trigger the background processor
    // Using watch channel so multiple triggers result in a single processing run
    if trigger.send(()).is_err() {
        error!("Failed to trigger background processor - receiver dropped");
    }

    Ok(())
}

/// Create a new invoice record for LUD-21 and NIP-57 support.
pub async fn create_invoice<DB>(
    db: &DB,
    payment_hash: &str,
    user_pubkey: &str,
    invoice: &str,
    invoice_expiry: i64,
) -> Result<(), LnurlRepositoryError>
where
    DB: LnurlRepository + Clone + Send + Sync + 'static,
{
    let now = now_millis();
    let invoice_record = Invoice {
        payment_hash: payment_hash.to_string(),
        user_pubkey: user_pubkey.to_string(),
        invoice: invoice.to_string(),
        preimage: None,
        invoice_expiry,
        created_at: now,
        updated_at: now,
    };
    db.upsert_invoice(&invoice_record).await?;
    debug!("Created invoice record for payment hash {}", payment_hash);
    Ok(())
}
