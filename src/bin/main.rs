#[macro_use]
extern crate quick_error;

use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use warp::{Filter, http::Response};

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
struct Post {
    content: String,
    hash: String
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
    entries: Vec<Post>,
    senders: Vec<Sender<String>>,
    time: SystemTime,
}
impl Entries {
    fn new(
        head: String,
        entries: Vec<Post>,
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
    println!("> Clearing db..");
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
    println!("> ok");

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
        .map(|post: Post, mtx: Arc<Mutex<Entries>>| {
            let (tx, rx): (Sender<String>, Receiver<String>) = mpsc::channel();
            let mut e = mtx.lock().unwrap();
            let hashed = hash(&post.content);
            let p = Post {
                content: post.content,
                hash: hashed
            };
            e.entries.push(p);
            e.senders.push(tx);
            drop(e);
            let result = rx.recv_timeout(Duration::from_millis(200));
            if let Ok(reply) = result {
                warp::reply::json(&Reply::new(reply))
            } else {
                println!("Timeout!");
                warp::reply::json(&Reply::new(String::from("timeout")))
            }
        });

    let mut hash_conn = pool.get().unwrap();
    let check = warp::path("check")
        .and(warp::any().map(move || pool.clone()))
        .map(|pool: r2d2::Pool<PostgresConnectionManager<NoTls>>| {
            let mut previous_head = String::from("ROOT");
            let mut check_conn = pool.get().unwrap();
            /*let rows = check_conn
                .query(
                "SELECT id, content, hash, chain_hash FROM chain order by id",
                &[],
            )
            .unwrap();
            let count = rows.len();
            for row in rows {
                let h1: String = row.get(2);
                let head: String = row.get(3);

                let expected = hash_head(&h1, &previous_head);
                previous_head = head.clone();
                assert_eq!(expected, head);
            }
            println!("ok {}", count);
            */
            Response::builder()
            .body("and a custom body")
        });


    println!("> chainer starting..");
    let mtx = mtx.clone();
    let _hasher = thread::spawn(move || {
        loop {
            let mut e = mtx.lock().unwrap();
            let elapsed = e.time.elapsed().unwrap().as_millis();
            let count = e.entries.len();
            if count > 50 || (elapsed > 100 && count > 0) {
                // print!("> chainer: write [count={}] [elapsed={}]..", count, elapsed);
                // let now = std::time::Instant::now();
                let result = write(&mut hash_conn, &e.entries, e.head.clone());
                if let Ok(hashes) = result {
                    let last = hashes[hashes.len() - 1].clone();
                    for (i, head) in hashes.into_iter().enumerate() {
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

    let future = warp::serve(check).run(([127, 0, 0, 1], 3030));
    // let routes = post.or(check);
    // let future = warp::serve(routes).run(([127, 0, 0, 1], 3030));
    println!("> warp starting..");
    
    runtime.block_on(future);
    // hasher.join().unwrap();
}

fn write(
    client: &mut PooledConnection<PostgresConnectionManager<NoTls>>,
    entries: &Vec<Post>,
    head: String,
) -> Result<Vec<String>, WriteError> {
    let mut tx = client.transaction()?;
    let mut writer = tx.copy_in("COPY chain(content, hash, chain_hash) FROM stdin")?;
    let mut head = head;
    let mut ret: Vec<String> = vec![];

    for post in entries {
        let next_head = hash_head(&post.hash, &head);
        let row = format!("{}\t{}\t{}\n", post.content, post.hash, next_head);
        writer.write_all(row.as_bytes())?;
        head = next_head;
        ret.push(head.clone());
    }
    writer.finish()?;
    tx.commit()?;
    Ok(ret)
}

pub fn hash_head(content_hash: &String, head: &String) -> String {
    let mut hasher = Sha512::new();
    hasher.update(content_hash.as_bytes());
    hasher.update(head.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn hash(content: &String) -> String {
    let mut hasher = Sha512::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}
