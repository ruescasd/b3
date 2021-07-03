// curl --header "Content-Type: application/json"   --request POST   --data '{"content":"xyz","h":"3"}'   http://localhost:3030/bb
#[macro_use]
extern crate quick_error;

use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use warp::Filter;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

use tokio::runtime::Builder;

use r2d2;
use r2d2::PooledConnection;
use r2d2_postgres::{postgres::Error, postgres::NoTls, PostgresConnectionManager};

quick_error! {
    #[derive(Debug)]
    pub enum WriteError {
        Empty{}
        PsqlError(err: Error) {
            from()
        }
        IOError(err: std::io::Error) {
            from()
        }
    }
}

#[derive(Deserialize, Serialize)]
struct Entry {
    content: String,
    h: String,
}
#[derive(Deserialize, Serialize)]
struct Reply {
    head: String,
}
impl Reply {
    fn new(head: String) -> Reply {
        Reply { head }
    }
}

struct Entries {
    head: String,
    entries: Vec<Entry>,
    senders: Vec<Sender<String>>,
    time: SystemTime,
}
impl Entries {
    fn new(
        head: String,
        entries: Vec<Entry>,
        senders: Vec<Sender<String>>,
        time: SystemTime,
    ) -> Entries {
        Entries {
            head,
            entries,
            senders,
            time,
        }
    }
    fn with_head(head: String) -> Entries {
        Entries::new(head, vec![], vec![], SystemTime::now())
    }
}
impl Default for Entries {
    fn default() -> Entries {
        Entries::new(String::from("ROOT"), vec![], vec![], SystemTime::now())
    }
}

fn main() {
    let manager =
        PostgresConnectionManager::new("host=localhost port=5433 user=b3".parse().unwrap(), NoTls);
    let pool = r2d2::Pool::new(manager).unwrap();

    let mut client = pool.get().unwrap();
    client.execute("drop table chain", &[]).unwrap();
    client
        .batch_execute(
            "
        CREATE TABLE chain (
            id          SERIAL PRIMARY KEY,
            content     TEXT NOT NULL,
            hash        TEXT NOT NULL,
            chain_hash  TEXT NOT NULL    
        )
    ",
        )
        .unwrap();

    let runtime = Builder::new_multi_thread()
        .worker_threads(100)
        .enable_io()
        .enable_time()
        .thread_name("worker")
        .thread_stack_size(3 * 1024 * 1024)
        .build()
        .unwrap();

    let mtx: Arc<Mutex<Entries>> = Arc::new(Mutex::new(Entries::default()));
    let mtx_request = mtx.clone();
    let post = warp::post()
        .and(warp::path("bb"))
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .and(warp::any().map(move || mtx_request.clone()))
        .map(|entry: Entry, mtx: Arc<Mutex<Entries>>| {
            let (tx, rx): (Sender<String>, Receiver<String>) = mpsc::channel();
            let mut e = mtx.lock().unwrap();
            e.entries.push(entry);
            e.senders.push(tx);
            drop(e);
            let result = rx.recv_timeout(Duration::from_millis(200));
            if let Ok(reply) = result {
                warp::reply::json(&Reply::new(reply))
            } else {
                warp::reply::json(&Reply::new(String::from("timeout")))
            }
        });

    println!("> chainer starting..");
    let mtx = mtx.clone();
    let mut hash_conn = pool.get().unwrap();
    let _hasher = thread::spawn(move || {
        loop {
            let mut e = mtx.lock().unwrap();
            let elapsed = e.time.elapsed().unwrap().as_millis();
            let count = e.entries.len();
            if count > 50 || (elapsed > 100 && count > 0) {
                // print!("> chainer: write [count={}] [elapsed={}]..", count, elapsed);
                // let now = std::time::Instant::now();
                let result = write(&mut hash_conn, &e.entries, e.head.clone());
                if let Ok(heads) = result {
                    let last = heads[heads.len() - 1].clone();
                    for (i, head) in heads.into_iter().enumerate() {
                        e.senders[i].send(head).unwrap();
                    }
                    *e = Entries::with_head(last);
                } else {
                    println!(">>> error {:?}", result);
                }
                // println!("done[{}ms]", now.elapsed().as_millis());
            }
            drop(e);
            // thread::sleep(Duration::from_millis(10));
        }
    });

    let future = warp::serve(post).run(([127, 0, 0, 1], 3030));
    println!("> warp starting..");
    runtime.block_on(future);
    // hasher.join().unwrap();
}

fn write(
    client: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
    entries: &Vec<Entry>,
    head: String,
) -> Result<Vec<String>, WriteError> {
    let mut tx = client.transaction()?;
    let mut writer = tx.copy_in("COPY chain(content, hash, chain_hash) FROM stdin")?;
    let mut head = head;
    let mut ret = vec![];

    for entry in entries {
        let (h1, h2) = hash(&entry.content, &head);
        let row = format!("{}\t{}\t{}\n", entry.content, h1, h2);
        writer.write_all(row.as_bytes())?;
        head = h2;
        ret.push(head.clone());
    }
    writer.finish()?;
    tx.commit()?;
    Ok(ret)
}

pub fn hash(content: &String, head: &String) -> (String, String) {
    let mut hasher = Sha512::new();
    hasher.update(content.as_bytes());
    let h1 = hex::encode(hasher.finalize());
    let mut hasher2 = Sha512::new();
    hasher2.update(h1.as_bytes());
    hasher2.update(head.as_bytes());
    let h2 = hex::encode(hasher2.finalize());

    (h1, h2)
}
