//! Immutable records plus one atomically replaced committed head. Orphan partial
//! records are never read as committed and are never silently repaired on reopen.
use super::*;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};

pub(super) struct Store {
    directory: PathBuf,
    head: Head,
    graph: DisplayGraph,
    _lock: File,
    failed: bool,
}
fn path(directory: &Path, name: &str) -> Result<PathBuf, Error> {
    let path = directory.join(name);
    if path.try_exists()?
        && (fs::symlink_metadata(&path)?.file_type().is_symlink()
            || path.canonicalize()?.parent() != Some(directory))
    {
        return Err(failure("display path escapes archive directory"));
    }
    Ok(path)
}
fn head(directory: &Path, scope: &DisplayScope) -> Result<Option<Head>, Error> {
    scope.validate()?;
    if !directory.try_exists()? {
        return Ok(None);
    }
    let directory = directory.canonicalize()?;
    let path = path(&directory, "head.json")?;
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let head: Head = serde_json::from_slice(&bytes)?;
    head.validate(scope)?;
    Ok(Some(head))
}
fn read_record(directory: &Path, sequence: u64) -> Result<Envelope, Error> {
    let path = path(directory, &format!("{sequence:020}.json"))?;
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn sync_directory(directory: &Path) -> Result<(), Error> {
    // Windows does not permit opening directories as ordinary File handles.
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    let _ = directory;
    Ok(())
}
fn publish_head(directory: &Path, head: &Head) -> Result<(), Error> {
    let temp = path(directory, &format!(".head-{}.tmp", uuid::Uuid::new_v4()))?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(&serde_json::to_vec(head)?)?;
    file.sync_all()?;
    fs::rename(temp, path(directory, "head.json")?)?;
    sync_directory(directory)
}
impl Store {
    pub(super) fn open(directory: &Path, scope: &DisplayScope) -> Result<Self, Error> {
        scope.validate()?;
        fs::create_dir_all(directory)?;
        let directory = directory.canonicalize()?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path(&directory, ".display.lock")?)?;
        lock.try_lock()
            .map_err(|_| failure("display archive already has an exclusive writer"))?;
        let head = match head(&directory, scope)? {
            Some(head) => head,
            None => {
                let head = Head::empty(scope.clone());
                publish_head(&directory, &head)?;
                head
            }
        };
        // Reopen validates the whole committed chain without retaining full history.
        let mut previous = None;
        let mut graph = DisplayGraph::empty(scope.clone());
        for sequence in 1..=head.sequence {
            let envelope = read_record(&directory, sequence)?;
            check(&envelope, sequence, previous.as_deref())?;
            graph.apply(&envelope.record)?;
            previous = Some(envelope.hash);
        }
        if previous != head.hash {
            return Err(failure("display committed head hash mismatch"));
        }
        Ok(Self {
            directory,
            head,
            graph,
            _lock: lock,
            failed: false,
        })
    }
}
impl DisplayStore for Store {
    fn append(&mut self, expected: u64, record: &ArchiveRecord) -> Result<(), Error> {
        if self.failed {
            return Err(failure("display writer failed; reopen required"));
        }
        validate_append(expected, &self.head, record)?;
        self.graph.validate(record)?;
        let result = (|| {
            let envelope = Envelope {
                previous: self.head.hash.clone(),
                hash: hash(self.head.hash.as_deref(), record)?,
                record: record.clone(),
            };
            let destination = path(&self.directory, &format!("{:020}.json", record.sequence))?;
            // Anything above the committed head is unacknowledged crash evidence.
            // Preserve it under a unique name; never overwrite committed records.
            if destination.try_exists()? {
                let orphan = path(
                    &self.directory,
                    &format!(".orphan-{}-{}.json", record.sequence, uuid::Uuid::new_v4()),
                )?;
                fs::rename(&destination, orphan)?;
            }
            let temporary = path(
                &self.directory,
                &format!(".record-{}.tmp", uuid::Uuid::new_v4()),
            )?;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&serde_json::to_vec(&envelope)?)?;
            file.sync_all()?;
            fs::rename(temporary, destination)?;
            sync_directory(&self.directory)?;
            let next = Head {
                sequence: record.sequence,
                hash: Some(envelope.hash),
                ..self.head.clone()
            };
            publish_head(&self.directory, &next)?;
            self.head = next;
            self.graph.apply(record)?;
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn revision(&mut self) -> Result<u64, Error> {
        Ok(self.head.sequence)
    }
    fn flush(&mut self) -> Result<(), Error> {
        if self.failed {
            return Err(failure("display writer failed; reopen required"));
        }
        sync_directory(&self.directory)
    }
}
pub(super) fn revision(directory: &Path, scope: &DisplayScope) -> Result<Option<u64>, Error> {
    Ok(head(directory, scope)?.map(|head| head.sequence))
}
pub(super) fn query(
    directory: &Path,
    scope: &DisplayScope,
    after: u64,
    high: Option<u64>,
    limit: usize,
) -> Result<Option<DisplayPage>, Error> {
    let Some(head) = head(directory, scope)? else {
        return Ok(None);
    };
    let high_water = page_bounds(&head, after, high, limit)?;
    let directory = directory.canonicalize()?;
    let mut previous = if after == 0 {
        None
    } else {
        let boundary = read_record(&directory, after)?;
        check(&boundary, after, boundary.previous.as_deref())?;
        Some(boundary.hash)
    };
    let mut records = Vec::new();
    let end = high_water.min(after.saturating_add(u64::try_from(limit).unwrap_or(u64::MAX)));
    if after < end {
        for sequence in (after + 1)..=end {
            let envelope = read_record(&directory, sequence)?;
            check(&envelope, sequence, previous.as_deref())?;
            previous = Some(envelope.hash);
            records.push(envelope.record);
        }
    }
    if end == head.sequence && previous != head.hash {
        return Err(failure("display committed head hash mismatch"));
    }
    let last = records.last().map_or(after, |record| record.sequence);
    Ok(Some(DisplayPage {
        records,
        high_water,
        next_cursor: (last < high_water).then_some(last),
    }))
}
