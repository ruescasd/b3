use warp::{Filter};
use warp::Rejection;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::time::Duration;
use std::time::SystemTime;

use tokio::runtime::Builder;
use tokio_postgres::{NoTls, Error};
use tokio::time::timeout;

use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use bb8::{Pool};
use bb8_postgres::PostgresConnectionManager;

#[derive(Debug)]
pub enum PostError {
    Empty,
    SendError,
    ReceiveError
}
impl warp::reject::Reject for PostError {}

use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, Receiver};

#[derive(Deserialize, Serialize, Debug)]
pub struct Post {
    content: String,
    hash: String
}
#[derive(Debug)]
pub struct Message {
    post: Post,
    sender: Sender<String>,
}
/*impl Message {
    fn new(
        post: Post,
        sender: Sender<String>,
    ) -> Message {
        Message {
            post,
            sender,
        }
    }
}*/ 
#[derive(Deserialize, Serialize)]
pub struct PostReply {
    head: String,
}
impl PostReply {
    fn new(head: String) -> PostReply {
        PostReply { head }
    }
}

async fn go(tx: Sender<Message>) {
    let post = warp::post()
        .and(warp::path("bb"))
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .and(warp::any().map(move || tx.clone()))
        .and_then(do_post);
        /*.map(|post: Post, tx: Sender<Message>| {
            async { 
                let result = do_post(post, tx).await;
                if let Ok(result) = result {
                    warp::reply::json(&Reply::new(result))
                } else {
                    println!("Timeout!");
                    warp::reply::json(&Reply::new(String::from("timeout")))
                }
            };
        });*/
    
    // GET /hello/warp => 200 OK with body "Hello, warp!"
    /* let hello = warp::path!("hello")
        .and(warp::any().map(move || pool.clone()))
        .map(|pool: bb8::Pool<PostgresConnectionManager<NoTls>>| {
            Response::builder().body("Hohoho")
        });


    warp::serve(hello)
        .run(([127, 0, 0, 1], 3030))
        .await;*/

    warp::serve(post)
        .run(([127, 0, 0, 1], 3030))
        .await;
}

async fn do_post(post: Post, tx: Sender<Message>) -> Result<impl warp::Reply, Rejection> {
    let (tx_back, mut rx_back): (Sender<String>, Receiver<String>) = mpsc::channel(100);
    let hashed = hash(&post.content);
    let p = Post {
        content: post.content,
        hash: hashed
    };
    let message = Message {
        post: p,
        sender: tx_back
    };
    tx.send(message).await.map_err(|e| warp::reject::custom(PostError::SendError))?;
    
    let result = timeout(Duration::from_millis(200), rx_back.recv()).await
        .map_err(|e| warp::reject::custom(PostError::ReceiveError))?;
    
    if let Some(head) = result {
        Ok(warp::reply::json(&PostReply::new(head)))
    } 
    else {
        println!("Timeout!");
        Ok(warp::reply::json(&PostReply::new(String::from("timeout"))))
    }
}

fn main() {
    let runtime = Builder::new_multi_thread()
    .enable_io()
    .enable_time()
    .thread_name("worker")
    .thread_stack_size(3 * 1024 * 1024)
    .build()
    .unwrap();

    let manager = PostgresConnectionManager::new("host=localhost port=5433 user=b3".parse().unwrap(), NoTls);
    let (tx, rx): (Sender<Message>, Receiver<Message>) = mpsc::channel(100);

    runtime.spawn(db_writer(manager.clone(), rx));

    runtime.block_on(go(tx));
}

async fn db_writer(manager: PostgresConnectionManager<NoTls>, rx: Receiver<Message>) {
    let pool = Pool::builder().build(manager).await.unwrap();
    
    println!("db_writer> Clearing db..");
    let mut conn: bb8::PooledConnection<PostgresConnectionManager<NoTls>> = pool.get().await.unwrap();
    conn.execute("drop table chain", &[]).await.unwrap();
    conn.batch_execute(
            "
        CREATE TABLE chain (
            id          SERIAL PRIMARY KEY,
            content     TEXT NOT NULL,
            hash        TEXT NOT NULL,
            chain_hash  TEXT NOT NULL    
        )
    ",
        )
        .await.unwrap();
    println!("db_writer> ok");


    let mut rx = rx;

    let mut posts = vec![];
    let mut senders = vec![];
    let mut last_insert = SystemTime::now();
    let mut head = String::from("ROOT");
    
    let mut idle = false;
    loop {
        match timeout(Duration::from_millis(10), rx.recv()).await {   
            Err(e) => {
                if idle == false && posts.len() == 0 {
                    println!("db_writer> Idle..");
                    idle = true;
                }
            },
            Ok(m) => {
                // println!("received data");
                let message = m.unwrap();
                posts.push(message.post);
                senders.push(message.sender);
                idle = false;
            },
        }
        let elapsed = last_insert.elapsed().unwrap().as_millis();
        if posts.len() > 50 || (elapsed > 100 && posts.len() > 0) {
            //println!("Doing shit now");
            let result = insert_rows(&mut conn, &posts, head.clone()).await;
            if let Ok(hashes) = result {
                let last = hashes[hashes.len() - 1].clone();
                for (i, head) in hashes.into_iter().enumerate() {
                    // FIXME this can break killing the thread
                    let result = senders[i].send(head).await;
                    match result {
                        Ok(_) => {
                            // print!(".");
                        }
                        Err(err) => {
                            println!("db_writer> error returning hash: {}", err);
                        }
                    }
                }
                head = last;
                last_insert = SystemTime::now();
            } else {
                println!("db_writer> insert error {:?}", result);
            }
            posts.clear();
            senders.clear();
        }
    }
}

async fn insert_rows(
    client: &mut bb8::PooledConnection<'_, PostgresConnectionManager<NoTls>>,
    entries: &Vec<Post>,
    head: String,
) -> Result<Vec<String>, Error> {
    
    let tx = client.transaction().await?;
    let sink = tx.copy_in("COPY chain(content, hash, chain_hash) FROM stdin BINARY").await?;
    let types = vec![Type::TEXT, Type::TEXT, Type::TEXT];
    let writer = BinaryCopyInWriter::new(sink, &types);
    let ret = write(writer, entries, head).await?;
    tx.commit().await?;
    
    Ok(ret)
}

use futures::pin_mut;

async fn write(writer: BinaryCopyInWriter, entries: &Vec<Post>, head: String) -> Result<Vec<String>, Error> {
    pin_mut!(writer);
    
    let mut head = head;
    let mut ret: Vec<String> = vec![];
    
    for post in entries {
        let mut row: Vec<&'_ (dyn ToSql + Sync)> = Vec::new();
        row.clear();
        let next_head = hash_head(&post.hash, &head);
        // println!("'{}' '{}' '{}'", &post.content, &post.hash, &next_head);
        row.push(&post.content);
        row.push(&post.hash);
        row.push(&next_head);
        writer.as_mut().write(&row).await?;
        // writer.as_mut().write_raw(row.as_bytes()).await?;
        head = next_head;
        ret.push(head.clone());
    }

    let written = writer.finish().await?;
    println!("wrote {} rows", written);


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

