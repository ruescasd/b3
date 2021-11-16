// derived from https://github.com/durch/rust-s3/blob/master/s3/bin/simple_crud.rs
extern crate s3;

use std::str;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use s3::S3Error;

const MESSAGE: &str = "I want to go to S3";

pub fn main() -> Result<(), S3Error> {
    
    // Load credentials directly
    let access_key = "minioadmin";
    let secret_key = "minioadmin";
    let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None).unwrap();
    
    let region = Region::Custom {
        region: "unused with minio".into(),
        endpoint: "http://localhost:9000".into(),
    };
    let bucket_name = "bucket1";
    let bucket = Bucket::new_with_path_style(bucket_name, region, credentials)?;
    let results = bucket.list_blocking("".to_string(), None)?;
    for (list, code) in results {
        assert_eq!(200, code);
        println!("bucket size = {:?}", list.contents.len());
    }

    let (_, code) = bucket.delete_object_blocking("test_file")?;
    println!("delete return code is {}", code);
    assert_eq!(204, code);
    
    // Put a "test_file" with the contents of MESSAGE at the root of the
    // bucket.
        
    let (_, code) = bucket.put_object_blocking("test_file", MESSAGE.as_bytes())?;
    // println!("{}", bucket.presign_get("test_file", 604801)?);
    assert_eq!(200, code);
    
    // Get the "test_file" contents and make sure that the returned message
    // matches what we sent.
    let (data, code) = bucket.get_object_blocking("test_file")?;
    let string = str::from_utf8(&data)?;
    // println!("{}", string);
    assert_eq!(200, code);
    assert_eq!(MESSAGE, string);

    Ok(())
}