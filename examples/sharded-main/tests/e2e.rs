use std::time::Duration;

#[test]
fn example_completes_with_expected_output() {
    let assertion = assert_cmd::Command::new(env!("CARGO_BIN_EXE_sharded-main"))
        .timeout(Duration::from_secs(20))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assertion.get_output().stdout);
    assert_eq!(stdout.matches("Produced: Item").count(), 12, "{stdout}");
    assert_eq!(stdout.matches("Produced items:").count(), 1, "{stdout}");
}
