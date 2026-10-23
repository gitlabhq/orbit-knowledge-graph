include!(concat!(env!("OUT_DIR"), "/query_contract.rs"));

#[cfg(test)]
mod tests {
    use strum::VariantNames;

    #[test]
    fn path_types_match_input() {
        assert_eq!(super::PATH_TYPES, crate::input::PathType::VARIANTS);
    }
}
