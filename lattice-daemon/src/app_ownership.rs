use crate::store::LocalRecordStore;
use lattice_core::app_namespace::APP_REGISTRY_PREFIX;
use lattice_core::identity::SignedRecord;
use lattice_core::registry::is_registry_operator;

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

    if key.starts_with(APP_REGISTRY_PREFIX) {
        if !is_registry_operator(&publisher_b64) {
            return Err(
                "app registry records may only be published by the Lattice operator".to_string(),
            );
        }
        return Ok(());
    }

    let existing_owner = local_record_store
        .get_app_record_owner(key)
        .map_err(|err| err.to_string())?;

    if existing_owner.as_deref() == Some(publisher_b64.as_str()) {
        return Ok(());
    }

    match existing_owner {
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
