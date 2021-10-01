use r2d2;
use r2d2::PooledConnection;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};

use sha2::{Digest, Sha512};

fn main() {
    let manager =
        PostgresConnectionManager::new("host=localhost port=5433 user=b3".parse().unwrap(), NoTls);
    let pool = r2d2::Pool::new(manager).unwrap();

    let mut client = pool.get().unwrap();
    let mut previous_head = String::from("ROOT");
    let rows = client
        .query(
            "SELECT id, content, hash, chain_hash FROM chain order by id",
            &[],
        )
        .unwrap();
    let count = rows.len();
    for row in rows {
        let h1: String = row.get(2);
        let head: String = row.get(3);

        let expected = hash(h1, previous_head);
        previous_head = head.clone();
        assert_eq!(expected, head);
    }
    drop(client);
    println!("ok {}", count);
}

pub fn hash(hash: String, head: String) -> String {
    let mut hasher = Sha512::new();
    hasher.update(hash.as_bytes());
    hasher.update(head.as_bytes());
    hex::encode(hasher.finalize())
}

fn list(client: &mut PooledConnection<PostgresConnectionManager<NoTls>>) {
    for row in client
        .query(
            "SELECT id, content, hash, chain_hash FROM chain order by id",
            &[],
        )
        .unwrap()
    {
        let id: i32 = row.get(0);
        let content: &str = row.get(1);
        let h1: &str = row.get(2);
        let h2: &str = row.get(3);

        println!("found: {} {} {} {}", id, content, h1, h2);
    }
}
