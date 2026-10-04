#[test]
fn invalid_macro_input_is_rejected() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}

#[test]
fn valid_macro_input_compiles_and_runs() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui-pass/*.rs");
}
