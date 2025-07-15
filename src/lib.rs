use anyhow::{anyhow, Result};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use base64;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeMap;
use std::env;
use std::io::{self, Write};
use std::path::Path;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

const S3_MAX_KEY_LENGTH_BYTES: usize = 1024;
const MAX_INDEX_LEN: usize = 6;
const DEFAULT_CONCURRENCY: usize = 8;

#[derive(Clone)]
pub struct VFS {
    client: Client,
    concurrency: usize,
}

impl VFS {
    pub async fn new() -> Result<Self> {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        let concurrency = get_concurrency();
        
        Ok(VFS { client, concurrency })
    }

    pub async fn encode(&self, input_path: &str, s3_uri: &str, force: bool) -> Result<()> {
        let (bucket, prefix) = parse_s3_path(s3_uri)?;

        // Check if objects already exist
        let exists = self.has_objects(&bucket, &prefix).await?;
        if exists && !force {
            print!("⚠️  S3 path s3://{}/{} already contains data. Overwrite? [y/N]: ", bucket, prefix);
            io::stdout().flush()?;
            
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let response = input.trim().to_lowercase();
            
            if response != "y" {
                println!("✋ Upload canceled.");
                return Ok(());
            }
        }

        if exists && force {
            self.delete(&format!("s3://{}/{}", bucket, prefix)).await
                .map_err(|e| anyhow!("Failed to delete existing prefix: {}", e))?;
        }

        let chunk_size = calculate_chunk_size(&prefix);
        if chunk_size < 1 {
            return Err(anyhow!("Calculated chunk size is too small for S3 key constraint"));
        }

        // Read file and create chunks
        let mut file = File::open(input_path).await?;
        let metadata = file.metadata().await?;
        let file_size = metadata.len() as usize;
        
        let total_chunks = if file_size % chunk_size == 0 {
            file_size / chunk_size
        } else {
            file_size / chunk_size + 1
        };

        let mut chunks = Vec::new();
        let mut buffer = vec![0u8; chunk_size];

        loop {
            let bytes_read = file.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            chunks.push(buffer[..bytes_read].to_vec());
        }

        println!("Uploading {} chunks...", chunks.len());

        // Upload chunks concurrently
        let mut futures = FuturesUnordered::new();
        let semaphore = tokio::sync::Semaphore::new(self.concurrency);

        for (index, chunk) in chunks.into_iter().enumerate() {
            let client = self.client.clone();
            let bucket = bucket.clone();
            let prefix = prefix.clone();
            let permit = semaphore.clone().acquire_owned().await?;
            
            let future = async move {
                let _permit = permit; // Hold permit until completion
                let encoded = base64::encode_config(&chunk, base64::URL_SAFE_NO_PAD);
                let key = format!("{}{}-{}", prefix, index + 1, encoded);
                
                let result = client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await;
                
                match result {
                    Ok(_) => {
                        print!("\rUploaded: {}/{}", index + 1, total_chunks);
                        io::stdout().flush().ok();
                        Ok(())
                    }
                    Err(e) => Err(anyhow!("Upload failed: {}", e)),
                }
            };
            
            futures.push(future);
        }

        // Wait for all uploads to complete
        let mut errors = Vec::new();
        while let Some(result) = futures.next().await {
            if let Err(e) = result {
                errors.push(e);
            }
        }

        println!("\n✅ Upload complete.");
        
        if let Some(first_error) = errors.into_iter().next() {
            Err(first_error)
        } else {
            Ok(())
        }
    }

    pub async fn restore(&self, s3_uri: &str, output_path: &str) -> Result<()> {
        let (bucket, prefix) = parse_s3_path(s3_uri)?;

        // List all objects and extract chunks
        let mut chunks = BTreeMap::new();
        
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self.client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix);
            
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            
            let response = req.send().await?;
            
            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        let name = key.strip_prefix(&prefix)
                            .unwrap_or(&key)
                            .trim_start_matches('/');
                        
                        if let Some((index_str, encoded)) = name.split_once('-') {
                            if let Ok(index) = index_str.parse::<usize>() {
                                chunks.insert(index, encoded.to_string());
                            }
                        }
                    }
                }
            }
            
            if !response.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = response.next_continuation_token;
        }

        if chunks.is_empty() {
            println!("⚠️  No chunks found at s3://{}/{}. Restore aborted.", bucket, prefix);
            return Ok(());
        }

        // Create output directory if needed
        if let Some(parent) = Path::new(output_path).parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut output_file = File::create(output_path).await?;

        println!("Downloading {} chunks...", chunks.len());

        // Download and decode chunks concurrently
        let mut futures = FuturesUnordered::new();
        let semaphore = tokio::sync::Semaphore::new(self.concurrency);

        for (index, encoded) in chunks {
            let permit = semaphore.clone().acquire_owned().await?;
            
            let future = async move {
                let _permit = permit;
                let data = base64::decode_config(&encoded, base64::URL_SAFE_NO_PAD)
                    .map_err(|e| anyhow!("Base64 decode failed: {}", e))?;
                
                print!("\rDownloaded: {}/{}", index, chunks.len());
                io::stdout().flush().ok();
                
                Ok::<(usize, Vec<u8>), anyhow::Error>((index, data))
            };
            
            futures.push(future);
        }

        // Collect results and sort by index
        let mut results = Vec::new();
        while let Some(result) = futures.next().await {
            results.push(result?);
        }
        
        results.sort_by_key(|(index, _)| *index);

        println!("\n✅ Download complete.");

        // Write chunks to output file in order
        for (_, data) in results {
            output_file.write_all(&data).await?;
        }

        println!("Restored file written to: {}", output_path);
        Ok(())
    }

    pub async fn delete(&self, s3_uri: &str) -> Result<()> {
        let (bucket, prefix) = parse_s3_path(s3_uri)?;

        let mut deleted = 0;
        let mut continuation_token: Option<String> = None;
        
        loop {
            let mut req = self.client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix);
            
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            
            let response = req.send().await?;
            
            if let Some(contents) = response.contents {
                let objects_to_delete: Vec<ObjectIdentifier> = contents
                    .into_iter()
                    .filter_map(|obj| obj.key.map(|key| ObjectIdentifier::builder().key(key).build()))
                    .collect();

                if !objects_to_delete.is_empty() {
                    let delete_request = Delete::builder()
                        .set_objects(Some(objects_to_delete.clone()))
                        .build();

                    self.client
                        .delete_objects()
                        .bucket(&bucket)
                        .delete(delete_request?)
                        .send()
                        .await?;

                    deleted += objects_to_delete.len();
                    print!("\rDeleted: {}", deleted);
                    io::stdout().flush().ok();
                }
            }
            
            if !response.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = response.next_continuation_token;
        }

        println!("\n✅ Delete complete.");
        Ok(())
    }

    async fn has_objects(&self, bucket: &str, prefix: &str) -> Result<bool> {
        let response = self.client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .max_keys(1)
            .send()
            .await?;

        Ok(response.contents.map_or(0, |c| c.len()) > 0)
    }
}

fn parse_s3_path(s3_path: &str) -> Result<(String, String)> {
    if !s3_path.starts_with("s3://") {
        return Err(anyhow!("Must start with s3://"));
    }

    let url = Url::parse(s3_path)?;
    let bucket = url.host_str()
        .ok_or_else(|| anyhow!("Invalid S3 URL: missing bucket"))?
        .to_string();
    
    let mut prefix = url.path().trim_start_matches('/').to_string();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }

    Ok((bucket, prefix))
}

fn calculate_chunk_size(prefix: &str) -> usize {
    let available = S3_MAX_KEY_LENGTH_BYTES
        .saturating_sub(prefix.len())
        .saturating_sub(MAX_INDEX_LEN);
    
    if available == 0 {
        return 0;
    }
    
    // Base64 encoding: 4 output chars for every 3 input bytes
    (available * 3) / 4
}

fn get_concurrency() -> usize {
    env::var("S3_CONCURRENCY")
        .ok()
        .and_then(|val| val.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_CONCURRENCY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_path() {
        let (bucket, prefix) = parse_s3_path("s3://my-bucket/path/to/folder/").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path/to/folder/");
    }

    #[test]
    fn test_calculate_chunk_size() {
        let prefix = "a".repeat(100) + "/";
        let size = calculate_chunk_size(&prefix);
        assert!(size > 0);
        assert!(size <= 768);
    }

    #[test]
    fn test_get_concurrency_default() {
        env::remove_var("S3_CONCURRENCY");
        let concurrency = get_concurrency();
        assert_eq!(concurrency, DEFAULT_CONCURRENCY);
    }

    #[test]
    fn test_get_concurrency_valid() {
        env::set_var("S3_CONCURRENCY", "5");
        let concurrency = get_concurrency();
        assert_eq!(concurrency, 5);
        env::remove_var("S3_CONCURRENCY");
    }

    #[test]
    fn test_get_concurrency_invalid() {
        env::set_var("S3_CONCURRENCY", "notanint");
        let concurrency = get_concurrency();
        assert_eq!(concurrency, DEFAULT_CONCURRENCY);
        env::remove_var("S3_CONCURRENCY");
    }
}