use std::time::Duration;

#[test]
fn example_completes_with_expected_output() {
    let assertion = assert_cmd::Command::new(env!("CARGO_BIN_EXE_no-main-shard-example"))
        .timeout(Duration::from_secs(20))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assertion.get_output().stdout);
    assert_eq!(stdout.matches("Bar received").count(), 3, "{stdout}");
    assert_eq!(stdout.matches("Bar moved").count(), 3, "{stdout}");
}
