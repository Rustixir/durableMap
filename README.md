# durableMap

durableMap is in-memory, durable storage
optimized for ( write-heavy, read-heavy )

# Why need to DurableMap ??

it is a concurrent hashMap using NonBlocking wal engine for persist to disk 
it avoid any loss data


# crates.io
https://crates.io/crates/rstorage


# Example

 ```rust
 
 #[tokio::main]
 async fn main() {
 
     
     // Optimized for write concurrent (API is same) 
    // let dl1 = WConcurrentStorage::<Document>::open("tester".to_owned(), 1000).await;
    
    // Optimized for read concurrent
    let dl = RConcurrentStorage::<Document>::open("tester".to_owned(), 1000).await;
 
    
    // Insert / Update
    let _ = dl.insert_entry(format!("102xa"), Document::new()).await;
    
    // Remove
    dl.remove_entry(&format("102xa")).await;

    // Read
    dl.table.read().await
            .iter()
            .for_each(|(key, doc)| {
                println!("==> {} -> {}", key, &doc.funame)
            });
            
 }
 
 
 #[derive(Serialize, Deserialize, Clone)]
 struct Document {
     funame: String,
     age: i32,
 }
 impl Document {
     pub fn new() -> Self {
         Document { 
             funame: String::from("DanyalMhai@gmail.com"), 
             age: 24, 
         }
     }
 }
 
 
 
``` 
 
