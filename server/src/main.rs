use core::sotradb::SotraDB;

#[tokio::main]
async fn main() {
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(100);

    std::thread::spawn(|| {
        let mut db = SotraDB::new("name-to-addresses");
    });

    // start server here
}
