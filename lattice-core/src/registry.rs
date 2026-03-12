pub const REGISTRY_OPERATOR_KEY_B64: &str = "smxmbFx2FnzeBIdzbnJcEyLcNF1mSnFWaALBuwWS0z8=";

pub fn is_registry_operator(publisher_key_b64: &str) -> bool {
    publisher_key_b64 == REGISTRY_OPERATOR_KEY_B64
}

#[cfg(test)]
mod tests {
    use super::{is_registry_operator, REGISTRY_OPERATOR_KEY_B64};

    #[test]
    fn accepts_operator_key() {
        assert!(is_registry_operator(REGISTRY_OPERATOR_KEY_B64));
    }

    #[test]
    fn rejects_other_keys() {
        assert!(!is_registry_operator(
            "Mz2BnWy56BEU4WJuTdwR709RtHvMav2eU9MNEmQmk00="
        ));
    }
}
