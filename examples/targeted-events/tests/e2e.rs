use std::time::Duration;

#[test]
fn topics_route_once_per_matching_subscriber_and_include_wildcards() {
    let assertion = assert_cmd::Command::new(env!("CARGO_BIN_EXE_targeted-events"))
        .timeout(Duration::from_secs(20))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assertion.get_output().stdout);
    assert!(
        stdout
            .contains("Totals: Orders: 3, Billing: 2, Shipping: 3, Orders or shipping: 4, All: 6"),
        "{stdout}"
    );
}
