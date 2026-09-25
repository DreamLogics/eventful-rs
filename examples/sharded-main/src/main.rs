//! Import product batches on a worker and update progress on the main thread.
//! Run with `cargo run -p sharded-main`.
use eventful_rs::*;
use std::cell::RefCell;
mod processor;
use processor::*;

declare_shard!(pub Main, runtime = main);

/// A main-thread projection of imported product names.
#[eventful(shard = Main)]
struct Progress {
    /// Names accepted by the listener.
    names: RefCell<Vec<String>>,
}
impl ImportEvents for Progress {
    fn imported(&self, name: String) {
        println!("Imported: {name}");
        self.names.borrow_mut().push(name);
    }
}

/// Drive main-thread callbacks while awaiting work on the import shard.
#[sharded_main(Main)]
async fn main() -> Result<(), DeliveryError> {
    let importer = Importer::new().await?;
    let progress = Progress::spawn(|| Progress {
        names: RefCell::default(),
        events: Default::default(),
    })
    .await?;
    let _subscription = importer.imported().connect(&progress).scoped();
    assert_eq!(
        importer
            .import(vec![" Apples ".into(), "".into(), "Pears".into()])
            .await?,
        2
    );
    // Tracked emission makes this read deterministic, without sleeping.
    let names = progress
        .deferred_upgrade_in_shard(async |p| p.names.borrow().clone())
        .await;
    assert_eq!(names, ["Apples", "Pears"]);

    // Multiple values on one shard can be inspected in a single callback.
    let second = Importer::new().await?;
    assert_eq!(second.import(vec!["Plums".into()]).await?, 1);
    let total = importer
        .join(&second)
        .ok_or(InvokeError::WrongShard)?
        .try_deferred_upgrade_in_shard(async |(a, b)| {
            println!("First batch: {}", a.report());
            a.count() + b.count()
        })
        .await?;
    assert_eq!(total, 3);
    importer.reset();
    // The following call starts after the synchronous reset on the same queue.
    assert_eq!(importer.count().await, 0);
    println!("Import complete: {total} products");
    Ok(())
}
