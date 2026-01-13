use anyhow::Result;
use std::path::{Path, PathBuf};
use std::fs;
use sha2::{Sha256, Digest};
use std::io::Read;

/// Manages caching of raw downloaded data
pub struct CacheManager {
    cache_dir: PathBuf,
}

impl CacheManager {
    /// Create a new cache manager with the specified cache directory
    pub fn new(cache_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&cache_dir)?;
        Ok(Self { cache_dir })
    }

    /// Get the cache path for a given URL
    pub fn get_cache_path(&self, url: &str) -> PathBuf {
        let hash = Self::hash_url(url);
        self.cache_dir.join(hash)
    }

    /// Check if a cached file exists and is valid
    pub fn is_cached(&self, url: &str) -> bool {
        self.get_cache_path(url).exists()
    }

    /// Hash a URL to create a unique cache key
    fn hash_url(url: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(url.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Read a cached file into memory
    pub fn read_cached(&self, url: &str) -> Result<Vec<u8>> {
        let path = self.get_cache_path(url);
        let mut file = fs::File::open(&path)?;
        let metadata = file.metadata()?;
        let mut buffer = Vec::with_capacity(metadata.len() as usize);
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    /// Write data to cache
    pub fn write_cached(&self, url: &str, data: &[u8]) -> Result<()> {
        let path = self.get_cache_path(url);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, data)?;
        Ok(())
    }

    /// Get the cache directory path
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }
}
