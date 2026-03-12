use base64::Engine as _;
use ed25519_dalek::SigningKey;
use lattice_core::identity::{canonical_json_bytes, SignedRecord};
use lattice_core::moderation::{ModerationEngine, ModerationRule, RuleAction, RuleKind};
use lattice_core::registry::{is_registry_operator, REGISTRY_OPERATOR_KEY_B64};
use lattice_daemon::app_registry::{AppRegistry, LocalAppRegistration};
use lattice_daemon::app_ownership::enforce_app_record_ownership;
use lattice_daemon::cache::{CachePolicy, SessionBlockCache};
use lattice_daemon::mime::{self, ALLOWED_MIME_TYPES, MAX_FILE_BYTES};
use lattice_daemon::moderation_helpers::{
    block_ingest_rule, hide_block_rule, hide_record_rule, ingest_rule, purge_local_matches,
    quarantine_record, republish_rule, validate_put_record_request,
};
use lattice_daemon::publish::validate_site_file_mime_policy;
use lattice_daemon::rpc::{self, RpcCommand};
use lattice_daemon::site_helpers::pin_cached_site_blocks;
use lattice_daemon::store::LocalRecordStore;
use lattice_site::manifest::{validate_app_manifest, AppManifest};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;
use std::process::Command;
use std::{fs, path::PathBuf};
use tempfile::tempdir;

fn rule(id: &str, kind: RuleKind, value: &str, action: RuleAction) -> ModerationRule {
    ModerationRule {
        id: id.to_string(),
        kind,
        value: value.to_string(),
        action,
        created_at: 1,
        note: None,
    }
}

fn signing_key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn signed_record(seed: u8, payload: serde_json::Value) -> SignedRecord {
    let payload = canonical_json_bytes(&payload).expect("encode canonical payload");
    SignedRecord::sign(&signing_key(seed), payload)
}

fn operator_signing_key() -> Option<SigningKey> {
    let home = std::env::var_os("HOME")?;
    let key_path: PathBuf = PathBuf::from(home).join(".lattice").join("site_signing.key");
    let bytes = fs::read(key_path).ok()?;
    let key_bytes: [u8; 32] = bytes.try_into().ok()?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    if is_registry_operator(&base64::engine::general_purpose::STANDARD.encode(signing_key.verifying_key().to_bytes())) {
        Some(signing_key)
    } else {
        None
    }
}

fn app_registration(site_name: &str, pid: u32) -> LocalAppRegistration {
    LocalAppRegistration {
        site_name: site_name.to_string(),
        proxy_port: 8890,
        proxy_paths: vec!["/api".to_string()],
        registered_at: 1,
        pid,
    }
}

fn signed_record_bytes(seed: u8, payload: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&signed_record(seed, payload)).expect("encode signed record")
}

#[test]
fn moderation_record_ingest_reject_rule_is_caught_by_validation_and_engine() {
    let key = "app:test-app:post:blocked";
    let value = serde_json::to_vec(&signed_record(7, serde_json::json!({"ok": true})))
        .expect("encode signed record");
    let engine = ModerationEngine::load(vec![rule(
        "reject-key",
        RuleKind::RecordKey,
        key,
        RuleAction::RejectIngest,
    )]);

    validate_put_record_request(key, &value)
        .expect("generic app key should pass validation");
    assert_eq!(engine.check_key(key), Some(&RuleAction::RejectIngest));
}

#[test]
fn first_app_put_stores_owner_key() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [1; 32]).expect("open store");
    let key = "app:my-app:post:lattice";
    let signed = signed_record(20, serde_json::json!({"fray": "lattice"}));
    let value = serde_json::to_vec(&signed).expect("encode record");

    enforce_app_record_ownership(&store, key, &value, 1_000).expect("first app claim");

    assert_eq!(
        store.get_app_record_owner(key).expect("load owner"),
        Some(signed.publisher_b64())
    );
}

#[test]
fn second_app_put_from_same_key_succeeds() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [2; 32]).expect("open store");
    let key = "app:my-app:post:lattice";
    let value = signed_record_bytes(21, serde_json::json!({"fray": "lattice"}));

    enforce_app_record_ownership(&store, key, &value, 1_000).expect("first app claim");
    enforce_app_record_ownership(&store, key, &value, 1_001).expect("same owner update");
}

#[test]
fn second_app_put_from_different_key_is_rejected() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [3; 32]).expect("open store");
    let key = "app:my-app:post:lattice";

    enforce_app_record_ownership(
        &store,
        key,
        &signed_record_bytes(22, serde_json::json!({"fray": "lattice"})),
        1_000,
    )
    .expect("first app claim");
    let err = enforce_app_record_ownership(
        &store,
        key,
        &signed_record_bytes(23, serde_json::json!({"fray": "lattice"})),
        1_001,
    )
    .expect_err("different owner should fail");
    assert_eq!(err, "app record owned by a different key");
}

#[test]
fn registry_record_put_rejects_non_operator_key() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [12; 32]).expect("open store");
    let key = "app:lattice:registry:fray";
    let value = signed_record_bytes(24, serde_json::json!({"app_id": "fray"}));

    let err = enforce_app_record_ownership(&store, key, &value, 1_000)
        .expect_err("non-operator registry publish should fail");
    assert_eq!(
        err,
        "app registry records may only be published by the Lattice operator"
    );
}

#[test]
fn registry_record_put_accepts_operator_key() {
    let Some(signing_key) = operator_signing_key() else {
        return;
    };
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [13; 32]).expect("open store");
    let key = "app:lattice:registry:fray";
    let payload = canonical_json_bytes(&serde_json::json!({"app_id": "fray"}))
        .expect("encode payload");
    let signed = SignedRecord::sign(&signing_key, payload);
    assert_eq!(signed.publisher_b64(), REGISTRY_OPERATOR_KEY_B64);
    let value = serde_json::to_vec(&signed).expect("encode record");

    enforce_app_record_ownership(&store, key, &value, 1_000)
        .expect("operator registry publish should succeed");
}

#[test]
fn non_app_keys_are_unaffected_by_ownership_enforcement() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [4; 32]).expect("open store");
    let site_value = br#"{"not":"signed"}"#.to_vec();
    let name_value = br#"{"not":"signed"}"#.to_vec();
    assert!(enforce_app_record_ownership(&store, "site:lattice", &site_value, 1_000).is_ok());
    assert!(enforce_app_record_ownership(&store, "name:lattice", &name_value, 1_000).is_ok());
}

#[test]
fn rate_limit_allows_first_claim() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [7; 32]).expect("open store");
    assert!(store
        .check_and_update_claim_rate_limit("key-a", 10_000)
        .is_ok());
}

#[test]
fn rate_limit_rejects_second_claim_within_24_hours() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [8; 32]).expect("open store");
    store
        .check_and_update_claim_rate_limit("key-a", 10_000)
        .expect("first claim");
    let err = store
        .check_and_update_claim_rate_limit("key-a", 10_100)
        .expect_err("second claim should fail");
    assert_eq!(err, "claim rate limit: one new claim per key per 24 hours");
}

#[test]
fn rate_limit_allows_claim_after_24_hours() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [9; 32]).expect("open store");
    store
        .check_and_update_claim_rate_limit("key-a", 10_000)
        .expect("first claim");
    store
        .check_and_update_claim_rate_limit("key-a", 10_000 + 86_400)
        .expect("claim after 24h");
}

#[test]
fn rate_limit_applies_across_name_and_fray_feed_claims() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [10; 32]).expect("open store");
    let signed = signed_record(28, serde_json::json!({"fray": "lattice"}));
    let publisher_b64 = signed.publisher_b64();

    store
        .check_and_update_claim_rate_limit(&publisher_b64, 20_000)
        .expect("name claim");
    let value = serde_json::to_vec(&signed).expect("encode record");
    let err = enforce_app_record_ownership(&store, "app:fray:feed:lattice", &value, 20_100)
        .expect_err("feed claim should share same rate limit");
    assert_eq!(err, "claim rate limit: one new claim per key per 24 hours");
}

#[test]
fn moderation_record_ingest_quarantine_rule_writes_entry() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [9; 32]).expect("open store");
    let signed = signed_record(11, serde_json::json!({"fray": "lattice"}));
    let value = serde_json::to_vec(&signed).expect("encode signed record");
    let key = "app:fray:feed:lattice";
    let engine = ModerationEngine::load(vec![rule(
        "quarantine-publisher",
        RuleKind::PublisherKey,
        &signed.publisher_b64(),
        RuleAction::Quarantine,
    )]);

    validate_put_record_request(key, &value).expect("signed app record accepted");
    let matched = ingest_rule(&engine, key, Some(&signed.publisher_b64()))
        .expect("publisher quarantine rule should match");
    assert_eq!(matched.action, RuleAction::Quarantine);

    quarantine_record(
        &store,
        matched,
        Some(key.to_string()),
        Some(signed.publisher_b64()),
        None,
        None,
    );

    let entries = store
        .list_quarantine_entries()
        .expect("read quarantine entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].matched_rule_id, "quarantine-publisher");
    assert_eq!(entries[0].record_key.as_deref(), Some(key));
}

#[test]
fn moderation_hide_record_rule_matches_only_target_key() {
    let key = "app:fray:feed:lattice";
    let engine = ModerationEngine::load(vec![rule(
        "hide-record",
        RuleKind::RecordKey,
        key,
        RuleAction::Hide,
    )]);

    let matched = hide_record_rule(&engine, key, None).expect("rule should match");
    assert_eq!(matched.id, "hide-record");
    assert!(hide_record_rule(&engine, "app:fray:feed:other", None).is_none());
}

#[test]
fn publisher_hide_rule_matches_site_manifest_publisher_key() {
    let site_key = "site:lattice";
    let publisher_b64 = signed_record(44, serde_json::json!({"publisher": "site"})).publisher_b64();
    let manifest_json = serde_json::json!({
        "name": "lattice",
        "version": 1,
        "publisher_key": hex::encode(
            base64::engine::general_purpose::STANDARD
                .decode(&publisher_b64)
                .expect("decode publisher")
        ),
        "rating": "general",
        "files": [],
        "signature": ""
    })
    .to_string();
    let manifest: lattice_site::manifest::SiteManifest =
        serde_json::from_str(&manifest_json).expect("decode manifest");
    let extracted_publisher = base64::engine::general_purpose::STANDARD.encode(
        hex::decode(&manifest.publisher_key).expect("decode manifest publisher hex"),
    );
    let engine = ModerationEngine::load(vec![rule(
        "hide-publisher",
        RuleKind::PublisherKey,
        &publisher_b64,
        RuleAction::Hide,
    )]);

    let matched = hide_record_rule(&engine, site_key, Some(&extracted_publisher))
        .expect("publisher hide rule should match");
    assert_eq!(matched.id, "hide-publisher");
}

#[test]
fn moderation_block_rules_cover_ingest_hide_and_republish() {
    let site_name = "lattice.loom";
    let reject_hash = "hash-reject";
    let hide_hash = "hash-hide";
    let site_key = format!("site:{site_name}");
    let engine = ModerationEngine::load(vec![
        rule(
            "reject-block",
            RuleKind::ContentHash,
            reject_hash,
            RuleAction::RejectIngest,
        ),
        rule(
            "hide-block",
            RuleKind::ContentHash,
            hide_hash,
            RuleAction::Hide,
        ),
        rule(
            "refuse-site",
            RuleKind::SiteName,
            site_name,
            RuleAction::RefuseRepublish,
        ),
    ]);

    let ingest = block_ingest_rule(&engine, site_name, reject_hash)
        .expect("block ingest rule should match");
    assert_eq!(ingest.id, "reject-block");
    assert!(block_ingest_rule(&engine, site_name, "other").is_none());

    let hide = hide_block_rule(&engine, site_name, hide_hash).expect("hide rule");
    assert_eq!(hide.id, "hide-block");

    let republish = republish_rule(&engine, &site_key, None, Some(site_name))
        .expect("republish rule should match");
    assert_eq!(republish.id, "refuse-site");
}

#[test]
fn publisher_refuse_republish_rule_matches_site_manifest_key() {
    let site_key = "site:lattice";
    let publisher_b64 = signed_record(45, serde_json::json!({"publisher": "site"})).publisher_b64();
    let engine = ModerationEngine::load(vec![rule(
        "refuse-publisher",
        RuleKind::PublisherKey,
        &publisher_b64,
        RuleAction::RefuseRepublish,
    )]);

    let matched = republish_rule(&engine, site_key, Some(&publisher_b64), Some("lattice"))
        .expect("publisher republish rule should match");
    assert_eq!(matched.id, "refuse-publisher");
}

#[tokio::test]
async fn moderation_purge_local_removes_matching_records_and_blocks() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [3; 32]).expect("open store");
    let mut local_records = std::collections::HashMap::new();
    let identity = libp2p::identity::Keypair::generate_ed25519();
    let peer_id = identity.public().to_peer_id();
    let mut kademlia = lattice_daemon::dht::new_kademlia(peer_id);

    let record_key = "app:fray:feed:lattice".to_string();
    let record_value = serde_json::to_vec(&signed_record(5, serde_json::json!({"fray": "lattice"})))
        .expect("encode record");
    store
        .put_record(&record_key, &record_value, false)
        .expect("persist record");
    local_records.insert(record_key.clone(), record_value.clone());

    purge_local_matches(
        &store,
        &mut local_records,
        &mut kademlia,
        &RuleKind::RecordKey,
        &record_key,
    )
    .expect("purge record");
    assert!(!store.load_records().expect("load records").contains_key(&record_key));

    let block_hash = "deadbeef";
    let block_bytes = b"secret pinned block";
    store
        .put_block(block_hash, block_bytes, "lattice.loom", CachePolicy::Pinned)
        .expect("persist block");
    purge_local_matches(
        &store,
        &mut local_records,
        &mut kademlia,
        &RuleKind::ContentHash,
        block_hash,
    )
    .expect("purge block");
    assert!(store.get_block(block_hash).expect("read block").is_none());

    let signed_a = signed_record(1, serde_json::json!({"fray": "a"}));
    let signed_b = signed_record(1, serde_json::json!({"fray": "b"}));
    let key_a = "app:fray:feed:a".to_string();
    let key_b = "app:fray:feed:b".to_string();
    let value_a = serde_json::to_vec(&signed_a).expect("encode record a");
    let value_b = serde_json::to_vec(&signed_b).expect("encode record b");
    store.put_record(&key_a, &value_a, false).expect("persist a");
    store.put_record(&key_b, &value_b, false).expect("persist b");
    local_records.insert(key_a.clone(), value_a);
    local_records.insert(key_b.clone(), value_b);

    purge_local_matches(
        &store,
        &mut local_records,
        &mut kademlia,
        &RuleKind::PublisherKey,
        &signed_a.publisher_b64(),
    )
    .expect("purge publisher");

    let records = store.load_records().expect("load remaining records");
    assert!(!records.contains_key(&key_a));
    assert!(!records.contains_key(&key_b));
}

#[test]
fn session_cache_enforces_byte_limit_and_evicts_lru() {
    let mut cache = SessionBlockCache::new(1024);
    cache.insert("a".to_string(), vec![1; 400]);
    assert_eq!(cache.get("a").expect("entry a"), &vec![1; 400]);

    cache.insert("b".to_string(), vec![2; 400]);
    cache.insert("c".to_string(), vec![3; 400]);

    assert!(cache.bytes() <= 1024);
    assert!(cache.get("a").is_none(), "oldest entry should have been evicted");
    assert!(cache.get("b").is_some());
    assert!(cache.get("c").is_some());
}

#[test]
fn block_cache_encryption_stores_ciphertext_and_roundtrips_plaintext() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [42; 32]).expect("open store");
    let hash = "feedface";
    let plaintext = b"known plaintext bytes";

    store
        .put_block(hash, plaintext, "lattice.loom", CachePolicy::Pinned)
        .expect("persist encrypted block");

    let raw = store
        .raw_block_bytes(hash)
        .expect("read raw block bytes")
        .expect("raw value present");
    assert_ne!(raw, plaintext);

    let decrypted = store
        .get_block(hash)
        .expect("read decrypted block")
        .expect("decrypted value present");
    assert_eq!(decrypted, plaintext);
}

#[test]
fn trusted_publishers_add_list_check_and_remove() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [5; 32]).expect("open store");
    let trusted_key = signed_record(19, serde_json::json!({"kind": "publisher"})).publisher_b64();
    let other_key = signed_record(20, serde_json::json!({"kind": "publisher"})).publisher_b64();

    store
        .add_trusted_publisher(
            trusted_key.clone(),
            "lattice.loom".to_string(),
            Some("operator trusted".to_string()),
        )
        .expect("add trusted publisher");

    assert!(store.is_trusted_publisher(&trusted_key).expect("trusted lookup"));
    assert!(!store.is_trusted_publisher(&other_key).expect("other lookup"));

    let listed = store
        .list_trusted_publishers()
        .expect("list trusted publishers");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].publisher_b64, trusted_key);

    assert!(store
        .remove_trusted_publisher(&listed[0].publisher_b64)
        .expect("remove trusted publisher"));
    assert!(!store
        .is_trusted_publisher(&listed[0].publisher_b64)
        .expect("post-remove lookup"));
    assert!(store
        .list_trusted_publishers()
        .expect("list after removal")
        .is_empty());
}

#[test]
fn known_publisher_status_transitions_first_seen_matches_and_key_changed() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [6; 32]).expect("open store");
    let site_name = "lattice";
    let key_a = signed_record(40, serde_json::json!({"publisher": "a"})).publisher_b64();
    let key_b = signed_record(41, serde_json::json!({"publisher": "b"})).publisher_b64();

    let first = store
        .record_known_publisher(site_name, &key_a)
        .expect("record first publisher");
    assert_eq!(first, rpc::KnownPublisherStatus::FirstSeen);

    let second = store
        .record_known_publisher(site_name, &key_a)
        .expect("record matching publisher");
    assert_eq!(second, rpc::KnownPublisherStatus::Matches);

    let changed = store
        .record_known_publisher(site_name, &key_b)
        .expect("record changed publisher");
    match changed {
        rpc::KnownPublisherStatus::KeyChanged {
            previous_key,
            first_seen_at,
        } => {
            assert_eq!(previous_key, key_a);
            assert!(first_seen_at > 0);
        }
        other => panic!("expected key change, got {other:?}"),
    }
}

#[test]
fn known_publisher_explicit_trust_persists() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [7; 32]).expect("open store");
    let site_name = "lattice";
    let publisher = signed_record(42, serde_json::json!({"publisher": "trusted"})).publisher_b64();

    store
        .record_known_publisher(site_name, &publisher)
        .expect("record publisher");
    store
        .set_explicitly_trusted(site_name, true)
        .expect("set trust");

    let known = store
        .get_known_publisher(site_name)
        .expect("get known publisher")
        .expect("known publisher exists");
    assert!(known.explicitly_trusted);
    assert!(known.explicitly_trusted_at.is_some());

    store
        .set_explicitly_trusted(site_name, false)
        .expect("clear trust");
    let updated = store
        .get_known_publisher(site_name)
        .expect("get updated known publisher")
        .expect("known publisher exists");
    assert!(!updated.explicitly_trusted);
    assert_eq!(updated.explicitly_trusted_at, None);
}

#[tokio::test]
async fn trust_site_rpc_with_pin_true_sets_trust_and_persists_blocks() {
    let dir = tempdir().expect("tempdir");
    let store = LocalRecordStore::open(dir.path(), [8; 32]).expect("open store");
    let site_name = "lattice";
    let publisher = signed_record(43, serde_json::json!({"publisher": "known"})).publisher_b64();
    store
        .record_known_publisher(site_name, &publisher)
        .expect("record known publisher");

    let block_hash = "hash-session";
    let block_bytes = b"session cached block".to_vec();
    let manifest_json = serde_json::json!({
        "name": site_name,
        "publisher_key": hex::encode(base64::engine::general_purpose::STANDARD.decode(&publisher).expect("decode publisher")),
        "version": 1,
        "rating": "general",
        "files": [
            {
                "path": "index.html",
                "mime_type": "text/html",
                "size": block_bytes.len(),
                "hash": block_hash,
                "chunks": []
            }
        ],
        "signature": ""
    })
    .to_string();

    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind temp port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);
        port
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel::<RpcCommand>(8);
    let server = lattice_daemon::rpc::start_rpc_server(port, tx)
        .await
        .expect("start rpc server");

    let loop_store = store;
    let manifest_for_loop = manifest_json.clone();
    let handler = tokio::spawn(async move {
        let mut session_cache = SessionBlockCache::new(1024 * 1024);
        session_cache.insert(block_hash.to_string(), block_bytes.clone());
        while let Some(command) = rx.recv().await {
            match command {
                RpcCommand::TrustSite {
                    name,
                    pin,
                    respond_to,
                } => {
                    let result = (|| -> Result<(), String> {
                        if pin {
                            let count = pin_cached_site_blocks(
                                &loop_store,
                                &mut session_cache,
                                &name,
                                &manifest_for_loop,
                            )
                            .map_err(|err| err.to_string())?;
                            if count == 0 {
                                return Err("no cached blocks found for site".to_string());
                            }
                        }
                        loop_store
                            .set_explicitly_trusted(&name, true)
                            .map_err(|err| err.to_string())?;
                        Ok(())
                    })();
                    let _ = respond_to.send(result);
                }
                RpcCommand::KnownPublisherStatus { name, respond_to } => {
                    let known = loop_store.get_known_publisher(&name).unwrap_or(None);
                    let _ = respond_to.send(known);
                }
                RpcCommand::ListPinned { respond_to } => {
                    let pinned = if loop_store.get_block(block_hash).unwrap_or(None).is_some() {
                        vec![site_name.to_string()]
                    } else {
                        Vec::new()
                    };
                    let _ = respond_to.send(pinned);
                }
                _ => panic!("unexpected rpc command in trust_site test"),
            }
        }
    });

    let client = HttpClientBuilder::default()
        .build(format!("http://127.0.0.1:{port}"))
        .expect("build http client");

    let response: serde_json::Value = client
        .request("trust_site", rpc_params! { site_name, true })
        .await
        .expect("trust_site rpc request");
    assert_eq!(response["status"], "ok");

    let known: Option<rpc::KnownPublisher> = client
        .request("known_publisher_status", rpc_params! { site_name })
        .await
        .expect("known_publisher_status request");
    let known = known.expect("known publisher response");
    assert!(known.explicitly_trusted);

    let pinned: Vec<String> = client
        .request("list_pinned", rpc_params! [])
        .await
        .expect("list_pinned request");
    assert_eq!(pinned, vec![site_name.to_string()]);

    server.stop().expect("stop rpc server");
    handler.abort();
}

#[test]
fn app_record_signature_validation_accepts_valid_and_rejects_tampering() {
    let payload = serde_json::json!({"type": "feed", "fray": "lattice"});
    let signed = signed_record(31, payload.clone());
    let signed_json = serde_json::to_vec(&signed).expect("serialize signed record");

    validate_put_record_request("app:fray:feed:lattice", &signed_json)
        .expect("valid signed app record");

    let mut tampered: serde_json::Value =
        serde_json::from_slice(&signed_json).expect("deserialize signed record json");
    if let Some(payload_b64) = tampered.get_mut("payload") {
        let replacement = canonical_json_bytes(&serde_json::json!({"type": "feed", "fray": "evil"}))
            .expect("encode tampered payload");
        *payload_b64 = serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(replacement));
    }
    let tampered_json = serde_json::to_vec(&tampered).expect("serialize tampered record");
    let err = validate_put_record_request("app:fray:feed:lattice", &tampered_json)
        .expect_err("tampered signature should fail");
    assert!(err.contains("signature verification failed"));

    let plain_json = br#"{"payload":"not signed"}"#.to_vec();
    let err = validate_put_record_request("app:fray:feed:lattice", &plain_json)
        .expect_err("plain json should fail SignedRecord decode");
    assert!(err.contains("SignedRecord JSON"));

    validate_put_record_request("name:foo", &signed_json)
        .expect("non-app keys do not require SignedRecord validation");
}

#[test]
fn mime_policy_detects_magic_bytes_fallbacks_and_violations() {
    let png_bytes = vec![
        0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 0,
    ];
    assert_eq!(mime::detect_mime("wrong.txt", &png_bytes), "image/png");

    let random_bytes = b"definitely not html but unknown bytes";
    assert_eq!(mime::detect_mime("test.html", random_bytes), "text/html");

    for allowed in ALLOWED_MIME_TYPES {
        assert_eq!(mime::violation_reason(allowed, MAX_FILE_BYTES), None);
    }
    assert_eq!(
        mime::violation_reason("text/html", MAX_FILE_BYTES + 1),
        Some("too_large")
    );
    assert_eq!(mime::violation_reason("video/mp4", 1024), Some("wrong_type"));

    let err = validate_site_file_mime_policy("movie.mp4", b"not really video", true)
        .expect_err("strict MIME policy should reject");
    assert!(err
        .to_string()
        .contains("rejected: movie.mp4"));
    validate_site_file_mime_policy("movie.mp4", b"not really video", false)
        .expect("warn mode should allow MIME violation");
}

#[test]
fn app_register_succeeds_with_valid_parameters() {
    let registry = AppRegistry::new();
    registry
        .register(app_registration("fray", std::process::id()))
        .expect("register app");

    let registered = registry.get("fray").expect("registered app");
    assert_eq!(registered.proxy_port, 8890);
    assert_eq!(registered.proxy_paths, vec!["/api".to_string()]);
}

#[test]
fn app_register_rejects_proxy_port_below_1024() {
    let registry = AppRegistry::new();
    let err = registry
        .register(LocalAppRegistration {
            proxy_port: 80,
            ..app_registration("fray", std::process::id())
        })
        .expect_err("low port should fail");
    assert!(err.contains("proxy_port"));
}

#[test]
fn app_register_rejects_proxy_paths_containing_dot_dot() {
    let registry = AppRegistry::new();
    let err = registry
        .register(LocalAppRegistration {
            proxy_paths: vec!["/api/../bad".to_string()],
            ..app_registration("fray", std::process::id())
        })
        .expect_err("dot dot path should fail");
    assert!(err.contains(".."));
}

#[test]
fn app_register_rejects_proxy_paths_not_starting_with_slash() {
    let registry = AppRegistry::new();
    let err = registry
        .register(LocalAppRegistration {
            proxy_paths: vec!["api".to_string()],
            ..app_registration("fray", std::process::id())
        })
        .expect_err("missing slash should fail");
    assert!(err.contains("start"));
}

#[test]
fn app_unregister_with_wrong_pid_fails() {
    let registry = AppRegistry::new();
    registry
        .register(app_registration("fray", std::process::id()))
        .expect("register app");

    let err = registry
        .unregister("fray", std::process::id().saturating_add(1))
        .expect_err("wrong pid should fail");
    assert!(err.contains("pid"));
}

#[test]
fn app_unregister_with_correct_pid_succeeds() {
    let registry = AppRegistry::new();
    let pid = std::process::id();
    registry
        .register(app_registration("fray", pid))
        .expect("register app");
    registry
        .unregister("fray", pid)
        .expect("unregister app");
    assert!(registry.get("fray").is_none());
}

#[test]
fn registering_same_site_from_different_live_pid_fails() {
    let registry = AppRegistry::new();
    registry
        .register(app_registration("fray", std::process::id()))
        .expect("register app");

    let mut child = Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("spawn sleep");
    let err = registry
        .register(app_registration("fray", child.id()))
        .expect_err("different live pid should fail");
    assert!(err.contains("another live process"));
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn manifest_validate_app_manifest_rejects_ports_below_1024() {
    let err = validate_app_manifest(&AppManifest {
        proxy_port: 80,
        proxy_paths: vec!["/api".to_string()],
    })
    .expect_err("low port should fail");
    assert!(err.contains("proxy_port"));
}

#[test]
fn manifest_validate_app_manifest_rejects_bad_path_prefixes() {
    let err = validate_app_manifest(&AppManifest {
        proxy_port: 8890,
        proxy_paths: vec!["api".to_string()],
    })
    .expect_err("missing leading slash should fail");
    assert!(err.contains("start"));

    let err = validate_app_manifest(&AppManifest {
        proxy_port: 8890,
        proxy_paths: vec!["/api/../bad".to_string()],
    })
    .expect_err("dot dot path should fail");
    assert!(err.contains(".."));
}
