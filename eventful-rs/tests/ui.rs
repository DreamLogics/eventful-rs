#[test]
fn invalid_macro_input_is_rejected() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
