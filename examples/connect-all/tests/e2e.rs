use std::time::Duration;

#[test]
fn all_events_follow_the_connection_group_lifetime() {
    let assertion = assert_cmd::Command::new(env!("CARGO_BIN_EXE_connect-all"))
        .timeout(Duration::from_secs(20))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assertion.get_output().stdout);
    assert_eq!(
        stdout,
        concat!(
            "Connected: 2 items, 1 batches\n",
            "Disconnected: 2 items, 1 batches\n",
            "Inside scope: 3 items, 2 batches\n",
            "After scope: 3 items, 2 batches\n",
            "Dropped plain group: 4 items, 3 batches\n",
        )
    );
}
