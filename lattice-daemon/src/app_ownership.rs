use crate::store::LocalRecordStore;
use lattice_core::app_namespace::APP_REGISTRY_PREFIX;
use lattice_core::identity::SignedRecord;
use lattice_core::registry::is_registry_operator;

enum ExistingAppRecordOwnership {
    OwnedBySigner,
    OwnedByOther,
    Unclaimed,
}

fn existing_app_record_ownership(
    local_record_store: &LocalRecordStore,
    key: &str,
    publisher_b64: &str,
) -> Result<ExistingAppRecordOwnership, String> {
    let existing_owner = local_record_store
        .get_app_record_owner(key)
        .map_err(|err| err.to_string())?;
    if let Some(owner) = existing_owner {
        return Ok(if owner == publisher_b64 {
            ExistingAppRecordOwnership::OwnedBySigner
        } else {
            ExistingAppRecordOwnership::OwnedByOther
        });
    }

    let Some(existing_value) = local_record_store
        .get_record(key)
        .map_err(|err| err.to_string())?
    else {
        return Ok(ExistingAppRecordOwnership::Unclaimed);
    };

    let existing_value_str = std::str::from_utf8(&existing_value)
        .map_err(|_| "invalid existing signed record".to_string())?;
    let existing_signed = serde_json::from_str::<SignedRecord>(existing_value_str)
        .map_err(|_| "invalid existing signed record".to_string())?;
    if !existing_signed.verify() {
        return Err("invalid existing signed record".to_string());
    }
    let existing_publisher_b64 = existing_signed.publisher_b64();
    if existing_publisher_b64 == publisher_b64 {
        local_record_store
            .set_app_record_owner(key, publisher_b64)
            .map_err(|err| err.to_string())?;
        Ok(ExistingAppRecordOwnership::OwnedBySigner)
    } else {
        Ok(ExistingAppRecordOwnership::OwnedByOther)
    }
}

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
    let signed = serde_json::from_str::<SignedRecord>(value_str)
        .map_err(|_| "invalid signed record".to_string())?;
    let publisher_b64 = signed.publisher_b64();

    if key.starts_with(APP_REGISTRY_PREFIX) {
        if !is_registry_operator(&publisher_b64) {
            return Err(
                "app registry records may only be published by the Lattice operator".to_string(),
            );
        }
        return Ok(());
    }

    match existing_app_record_ownership(local_record_store, key, &publisher_b64)? {
        ExistingAppRecordOwnership::OwnedBySigner => Ok(()),
        ExistingAppRecordOwnership::OwnedByOther => {
            Err("app record owned by a different key".to_string())
        }
        ExistingAppRecordOwnership::Unclaimed => {
            local_record_store.check_and_update_claim_rate_limit(&publisher_b64, now)?;
            local_record_store
                .set_app_record_owner(key, &publisher_b64)
                .map_err(|err| err.to_string())?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::enforce_app_record_ownership;
    use crate::store::LocalRecordStore;
    use ed25519_dalek::SigningKey;
    use lattice_core::identity::SignedRecord;
    use tempfile::tempdir;

    fn signing_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn signed_record_json(seed: u8, payload: &[u8]) -> Vec<u8> {
        serde_json::to_vec(&SignedRecord::sign(&signing_key(seed), payload.to_vec()))
            .expect("serialize signed record")
    }

    #[test]
    fn reclaiming_owned_fray_identity_skips_claim_rate_limit() {
        let dir = tempdir().expect("tempdir");
        let store = LocalRecordStore::open(dir.path(), [11; 32]).expect("open store");
        let key = "app:fray:identity:alice";
        let first_value = signed_record_json(7, br#"{"handle":"alice","display_name":"Alice"}"#);
        store
            .put_record(key, &first_value, false)
            .expect("seed record");

        let owner_b64 = SignedRecord::sign(&signing_key(7), b"{}".to_vec()).publisher_b64();
        store
            .set_last_claim_ts(&owner_b64, 20_000)
            .expect("seed claim window");

        let updated_value =
            signed_record_json(7, br#"{"handle":"alice","display_name":"Alice Z"}"#);
        enforce_app_record_ownership(&store, key, &updated_value, 30_000)
            .expect("owned update should not rate limit");

        let last_claim = store
            .get_last_claim_ts(&owner_b64)
            .expect("read claim ts")
            .expect("claim ts present");
        assert_eq!(last_claim, 20_000);
        assert_eq!(
            store.get_app_record_owner(key).expect("read owner"),
            Some(owner_b64),
        );
    }
}
