use std::time::Duration;

#[test]
fn imports_normalized_products_and_completes_tracked_delivery() {
    let assertion = assert_cmd::Command::new(env!("CARGO_BIN_EXE_sharded-main"))
        .timeout(Duration::from_secs(20))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assertion.get_output().stdout);
    assert_eq!(stdout.matches("Imported: ").count(), 2, "{stdout}");
    assert!(stdout.contains("Imported: Apples"), "{stdout}");
    assert!(stdout.contains("Imported: Pears"), "{stdout}");
    assert!(stdout.contains("Import complete: 3 products"), "{stdout}");
}
