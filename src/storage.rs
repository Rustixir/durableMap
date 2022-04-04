pub mod storage {
    
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use serde_derive::{Serialize, Deserialize};
    use tokio::sync::{oneshot::channel, mpsc};

    use dashmap::DashMap;

    use crate::async_disk_log::{self as async_disk_log, Job};



    #[derive(Serialize, Deserialize)]
    pub enum Query<Document> {
        Insert(Key, Document),
        Remove(Key)
    }


    type Key = String;


    pub struct Storage<Document> {
        pub table: DashMap<String, Document>,
        wal_chan: mpsc::Sender<async_disk_log::Job>
    }


    impl<'a, Document> Storage<Document> 
    where
        Document: Serialize + DeserializeOwned + Clone
    {
        
        /// 1. check if exist Load entry
        /// 2. check if not exist
        ///      > create wal_chan for it   
        pub fn open(path: String, table_name: String) -> Self {
            
            let chan = async_disk_log::start(path, table_name, 500);

            Storage { 
                table: DashMap::new(),
                wal_chan: chan, 
            }
        }


        pub async fn insert_entry(&self, key: Key, doc: Document) -> Option<Document> {
            let query = Query::Insert(key.clone(), doc.clone());
            let _ = self.wal_chan.send(Job::WriteLog { data: bincode::serialize(&query).unwrap(), dst: None }).await;
            self.table.insert(key, doc)
        }


        pub async fn remove_entry(&self, key: Key) {
            match self.table.remove(&key) {
                Some(_) => {
                    let query = Query::<Document>::Remove(key);
                    let _ = self.wal_chan.send(Job::WriteLog { data: bincode::serialize(&query).unwrap(), dst: None }).await;
                }
                None => {
                    
                }
            }
        }



        pub async fn restore_storage(&self)  {

            let mut segment_index = 1;
            loop {
                
                let (sx, rx) = channel();

                match self.wal_chan.send(Job::GetSegment { segment_index, dst: sx }).await {
                    Ok(_) => {
                        match rx.await {
                            Ok(res) => {
                                match res {
                                    Ok(mut log) => {
                                        segment_index += 1;
                                        let iter = log.iter(..).unwrap();

                                        for qline in iter {
                                            let query: Query<Document> = bincode::deserialize(&qline.unwrap()).unwrap();
                                            match query {
                                                Query::Insert(key, doc) => {
                                                    self.table.insert(key, doc);
                                                }
                                                Query::Remove(k) => {
                                                    self.table.remove(&k);
                                                }
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        println!("ERROR: {}", err.to_string());
                                        return;
                                    }
                                }
                            }
                            Err(_) => {
                                println!("Finished");
                                return;        
                            }
                        }
                    }
                    Err(err) => {
                        println!("ERROR: {}", err.to_string());
                        return;
                    }
                }


            }
        }
        
    }


}