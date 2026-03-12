use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedRecord {
    pub publisher_key: VerifyingKey,
    pub payload: Vec<u8>,
    pub signature: Signature,
    pub signed_at: u64,
}

impl SignedRecord {
    pub fn sign(signing_key: &SigningKey, payload: Vec<u8>) -> Self {
        Self {
            publisher_key: signing_key.verifying_key(),
            signature: signing_key.sign(&payload),
            payload,
            signed_at: now_secs(),
        }
    }

    pub fn verify(&self) -> bool {
        self.publisher_key
            .verify(&self.payload, &self.signature)
            .is_ok()
    }

    pub fn publisher_b64(&self) -> String {
        BASE64_STANDARD.encode(self.publisher_key.as_bytes())
    }

    pub fn payload_json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.payload)
    }
}

pub fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(value)
}

impl Serialize for SignedRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SignedRecord", 4)?;
        state.serialize_field(
            "publisher_key",
            &BASE64_STANDARD.encode(self.publisher_key.as_bytes()),
        )?;
        state.serialize_field("payload", &BASE64_STANDARD.encode(&self.payload))?;
        state.serialize_field(
            "signature",
            &BASE64_STANDARD.encode(self.signature.to_bytes()),
        )?;
        state.serialize_field("signed_at", &self.signed_at)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for SignedRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            PublisherKey,
            Payload,
            Signature,
            SignedAt,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("a SignedRecord field")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "publisher_key" => Ok(Field::PublisherKey),
                            "payload" => Ok(Field::Payload),
                            "signature" => Ok(Field::Signature),
                            "signed_at" => Ok(Field::SignedAt),
                            _ => Err(de::Error::unknown_field(
                                value,
                                &["publisher_key", "payload", "signature", "signed_at"],
                            )),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct SignedRecordVisitor;

        impl<'de> Visitor<'de> for SignedRecordVisitor {
            type Value = SignedRecord;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a SignedRecord JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut publisher_key = None;
                let mut payload = None;
                let mut signature = None;
                let mut signed_at = None;

                while let Some(field) = map.next_key()? {
                    match field {
                        Field::PublisherKey => {
                            let value: String = map.next_value()?;
                            publisher_key =
                                Some(decode_verifying_key(&value).map_err(de::Error::custom)?);
                        }
                        Field::Payload => {
                            let value: String = map.next_value()?;
                            payload = Some(BASE64_STANDARD.decode(value).map_err(|err| {
                                de::Error::custom(format!("invalid payload base64: {err}"))
                            })?);
                        }
                        Field::Signature => {
                            let value: String = map.next_value()?;
                            signature = Some(decode_signature(&value).map_err(de::Error::custom)?);
                        }
                        Field::SignedAt => {
                            signed_at = Some(map.next_value()?);
                        }
                    }
                }

                Ok(SignedRecord {
                    publisher_key: publisher_key
                        .ok_or_else(|| de::Error::missing_field("publisher_key"))?,
                    payload: payload.ok_or_else(|| de::Error::missing_field("payload"))?,
                    signature: signature.ok_or_else(|| de::Error::missing_field("signature"))?,
                    signed_at: signed_at.ok_or_else(|| de::Error::missing_field("signed_at"))?,
                })
            }
        }

        deserializer.deserialize_struct(
            "SignedRecord",
            &["publisher_key", "payload", "signature", "signed_at"],
            SignedRecordVisitor,
        )
    }
}

fn decode_verifying_key(value: &str) -> Result<VerifyingKey, String> {
    let bytes = BASE64_STANDARD
        .decode(value)
        .map_err(|err| format!("invalid publisher_key base64: {err}"))?;
    let array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "publisher_key must be exactly 32 bytes".to_string())?;
    VerifyingKey::from_bytes(&array).map_err(|err| format!("invalid verifying key bytes: {err}"))
}

fn decode_signature(value: &str) -> Result<Signature, String> {
    let bytes = BASE64_STANDARD
        .decode(value)
        .map_err(|err| format!("invalid signature base64: {err}"))?;
    Signature::from_slice(&bytes).map_err(|err| format!("invalid signature bytes: {err}"))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct CanonicalPayload {
        version: u8,
        fray: &'static str,
    }

    fn signing_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    #[test]
    fn verifies_signed_record_and_roundtrips_json() {
        let key = signing_key(7);
        let payload = canonical_json_bytes(&CanonicalPayload {
            version: 1,
            fray: "lattice",
        })
        .expect("encode payload");
        let signed = SignedRecord::sign(&key, payload);
        assert!(signed.verify());

        let encoded = serde_json::to_string(&signed).expect("serialize signed record");
        let decoded: SignedRecord =
            serde_json::from_str(&encoded).expect("deserialize signed record");
        assert!(decoded.verify());
        assert_eq!(decoded.publisher_b64(), signed.publisher_b64());
    }

    #[test]
    fn rejects_tampered_payload() {
        let key = signing_key(3);
        let mut signed = SignedRecord::sign(&key, b"{\"ok\":true}".to_vec());
        signed.payload = b"{\"ok\":false}".to_vec();
        assert!(!signed.verify());
    }

    #[test]
    fn rejects_tampered_pubkey() {
        let key = signing_key(4);
        let mut signed = SignedRecord::sign(&key, b"{\"ok\":true}".to_vec());
        signed.publisher_key = signing_key(5).verifying_key();
        assert!(!signed.verify());
    }
}
