pub mod disk_log {

    use std::sync::Mutex;
    use std::sync::Arc;
    use std::{path::Path, fs};
    use std::io::Error;
    
    use simple_wal::LogFile;
    use simple_wal::LogError;
    
    use tokio::sync::{mpsc, oneshot};
    
    
    struct TmpLogStruct {
        path: String,
        log: LogFile,
        current_segment_index: usize
    }    




    pub enum Job {

        WriteLog {
            data: Vec<u8>,
            dst:  Option<oneshot::Sender<Result<(), StatusResult>>>
        },
        
        GetSegment {
            segment_index: usize, 
            dst: oneshot::Sender<Result<LogFile, LogError>>,
        },

    }

    
    pub enum StatusResult {
        LogErr(LogError),
        IoError(Error),
    }


    pub fn start(path: String, table_name: String, total_segment_size: usize) 
        -> mpsc::Sender<Job>
    {
        
        let (sx, mut rx) = mpsc::channel::<Job>(20);
        tokio::task::spawn(async move {
            let ctx = Arc::new(Mutex::new(Context::open(&path, &table_name, total_segment_size).unwrap()));
            while let Some(job) = rx.recv().await {
                match job {
                    Job::WriteLog { mut data, 
                                    dst } => {
                                    
                        let ctx_copy = ctx.clone();
                        let resp = tokio::task::spawn_blocking(move || {
                            let mut ctx = ctx_copy.lock().unwrap();
                            ctx.write_to_disk(&mut data)
                        });
                    
                        
                        match dst {
                            Some(dst) => {
                                let resp = resp.await.unwrap();
                                let _ = dst.send(resp);
                            }
                            None => (),
                        }
                    
                    }
                    
                    
                    Job::GetSegment { segment_index, dst } => {
                        let cx = ctx.lock().unwrap();
                        let filename = cx.find_filename(segment_index);
                        match LogFile::open(filename) {
                            Ok(log) => {
                                let _ = dst.send(Ok(log));
                            }
                            Err(e) => {
                                let _ = dst.send(Err(e));
                            }
                        }
                    }
                }
            }
        });


        sx
    }


     // -----------------------------------------

     struct Context {
        log: LogFile,
        path: String,

        // total_segment_size is total len of query list per file.LOG  
        total_segment_size: usize,

        // used_segment is len of reocrd in current_file.LOG 
        used_segment: usize,

        // current_segment is pointer to current_segment
        current_segment_index: usize

    }
    impl Context {

        pub fn open(path: &str, 
                    table_name: &str, 
                    mut total_segment_size: usize) -> Result<Self, LogError> 
        {
            // at-least 200 Record
            total_segment_size =  if total_segment_size < 200 { 200 } else { total_segment_size };
            

            let mut slog = open_last_segment(path, table_name, total_segment_size);
            
            let used_segment = used_segment(&mut slog.log);

            Ok(Context{
                log: slog.log,

                path: slog.path,
                
                total_segment_size,

                used_segment,

                // current_segment is pointer to current_segment and when move to new segment change
                current_segment_index: 1
            })
        }
     

        #[inline]
        fn write_to_disk(&mut self, bytes: &mut Vec<u8>) -> Result<(), StatusResult> {
            let sum = self.used_segment + 1;

            // if segment have free space 
            if sum < self.total_segment_size {
                let _ = self.log.write(bytes);

                // flush to disk
                match self.log.flush() {
                    Ok(_) => {
                        self.used_segment = sum;
                        Ok(())
                    }
                    Err(err) => {
                        return Err(StatusResult::IoError(err))
                    }
                }
                
            }
            // if segment with this become full
            else if sum == self.total_segment_size {
                // write buffer to segment
                let _ = self.log.write(bytes);
 
                // flush to disk
                match self.log.flush() {
                    Ok(_) => {
 
                        // ----- move to new segment -----
                        self.current_segment_index += 1;
 
                        // create filename for new segment
                        let curr_filename = filename_factory(&self.path, self.current_segment_index * self.total_segment_size);
 
                        // create new segment
                        let log = match LogFile::open(&curr_filename) {
                            Ok(lf) => lf,
                            Err(err) => {
                                return Err(StatusResult::LogErr(err))
                            }
                        };
 
                        self.used_segment = 0;
                        self.log = log;
 
                        Ok(())
                    }
                    Err(err) => {
                        return Err(StatusResult::IoError(err))
                    }
                }
            }
            // if segment is full
            else {

                // ----- move to new segment -----
                self.current_segment_index += 1;

                // create filename for new segment
                let curr_filename = filename_factory(&self.path, self.current_segment_index * self.total_segment_size);
                
                
                // create new segment
                let log = match LogFile::open(&curr_filename) {
                    Ok(lf) => lf,
                    Err(err) => {
                        return Err(StatusResult::LogErr(err))
                    }
                };
            
                self.log = log;       
            
                // write buffer to segment
                let _ = self.log.write(bytes);
                // flush to disk
                match self.log.flush() {
                    Ok(_) => {
                        self.used_segment = 1;
                        Ok(())
                    }
                    Err(err) => {
                        return Err(StatusResult::IoError(err))
                    }
                } 

            }
        }


        #[inline]
        fn find_filename(&self, segment_index: usize) -> String {
            filename_factory(&self.path, self.total_segment_size * segment_index)
        }
    
    }
     
    
    // -------------------------------------------------

   
    #[inline]
    fn filename_factory(path: &str, segment_pointer: usize) -> String {
        format!("{}/segment-{}.LOG", &path, segment_pointer)
    }

    // if not exist directory then is first time run, create dir and a segment-1 and open it
    // else open latest segment exist
    fn used_segment(log: &mut LogFile) -> usize {

        let mut counter = 0;
        let iter = log.iter(..).unwrap();
        iter.for_each(|_| counter += 1);

        return counter
    }

    fn open_last_segment(path: &str, table_name: &str, total_segment_size: usize) -> TmpLogStruct {
         let path = format!("{}/{}", path, table_name);
         // if not exist, (First times is started_service)
         if !Path::new(&path).is_dir() {
             let curr_filename = filename_factory(&path, total_segment_size);
             fs::create_dir(&path).unwrap();
             return TmpLogStruct {
                 path: path,
                 log: LogFile::open(&curr_filename).unwrap(),
                 current_segment_index: 1
             }
         }
         else {
             // -----------------------------------------
             let mut segment_index = 2;
             let mut latest_segment = filename_factory(&path, total_segment_size); 
             loop {        

                 let segment_pointer = total_segment_size * segment_index;
                 let filename = filename_factory(&path, segment_pointer);
                
                 if Path::new(&filename).is_file() {
                     latest_segment = filename;
                     segment_index += 1; 
                 } 
                 else {
                     // if not exist segment_2
                     if segment_index == 2 {
                         return TmpLogStruct {
                             path,
                             log: LogFile::open(latest_segment).unwrap(),
                             current_segment_index: 1
                         }
                     } 
                     else {
                         return TmpLogStruct {
                             path,
                             log: LogFile::open(latest_segment).unwrap(),
                             current_segment_index: (segment_index - 1)
                         }
                     }
                 }                
             }
         }

    }


}