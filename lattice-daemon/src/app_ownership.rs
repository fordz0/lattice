use crate::store::LocalRecordStore;
use lattice_core::identity::SignedRecord;

pub fn enforce_app_record_ownership(
    local_record_store: &LocalRecordStore,
    key: &str,
    value: &[u8],
    now: u64,
) -> Result<(), String> {
    if !key.starts_with("app:") {
        return Ok(());
    }

    let value_str = std::str::from_utf8(value).map_err(|_| "invalid signed record".to_string())?;
    let signed =
        serde_json::from_str::<SignedRecord>(value_str).map_err(|_| "invalid signed record".to_string())?;
    let publisher_b64 = signed.publisher_b64();

    match local_record_store
        .get_app_record_owner(key)
        .map_err(|err| err.to_string())?
    {
        Some(owner) if owner == publisher_b64 => Ok(()),
        Some(_) => Err("app record owned by a different key".to_string()),
        None => {
            local_record_store.check_and_update_claim_rate_limit(&publisher_b64, now)?;
            local_record_store
                .set_app_record_owner(key, &publisher_b64)
                .map_err(|err| err.to_string())?;
            Ok(())
        }
    }
}
