use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::ENOENT;
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};
use std::{env, error::Error};

// Kubernetes related imports
#[allow(unused_imports)]
use k8s_openapi::api::core::v1::Namespace;
use kube::Client;

use kube::api::{Api, ListParams, Meta};

const TTL: Duration = Duration::from_secs(1); // 1 second

const HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
    padding: 0,
};

const HELLO_TXT_CONTENT: &str = "Hello World!\n";

const HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 13,
    blocks: 1,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
    padding: 0,
};

struct HelloFS;

impl Filesystem for HelloFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 && name.to_str() == Some("hello.txt") {
            reply.entry(&TTL, &HELLO_TXT_ATTR, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match ino {
            1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
            2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
            _ => reply.error(ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        reply: ReplyData,
    ) {
        if ino == 2 {
            reply.data(&HELLO_TXT_CONTENT.as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        let mut entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
            (2, FileType::RegularFile, "hello.txt"),
        ];

        let mut inode_idx = entries.len() as u64;
        let namespaces = get_namespaces_blocking().unwrap();
        for ns in &namespaces {
            entries.push((inode_idx, FileType::Directory, ns));
            inode_idx += 1;
        }

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            reply.add(entry.0, (i + 1) as i64, entry.1, entry.2);
        }
        reply.ok();
    }
}

// Returns a list with the names of all namespaces.
async fn get_namespaces() -> Result<Vec<String>, Box<dyn Error>> {
    let client = Client::try_default().await?;

    let ns_api: Api<Namespace> = Api::all(client);
    let lp = ListParams::default();
    let mut ns_names: Vec<String> = vec![];

    for namespace in ns_api.list(&lp).await? {
        ns_names.push(String::from(Meta::name(&namespace)));
    }

    Ok(ns_names)
}

// Ideally I would like to annotate this function (the called function) and
// automatically generate the blocking version of it.
#[tokio::main]
async fn get_namespaces_blocking() -> Result<Vec<String>, Box<dyn Error>> {
    get_namespaces().await
}

fn main() -> Result<(), Box<dyn Error>> {
    let mountpoint = env::args_os().nth(1).unwrap();
    let options = vec![
        MountOption::RO,
        MountOption::FSName("hello".to_string()),
        MountOption::AutoUnmount,
    ];

    fuser::mount2(HelloFS, mountpoint, &options).unwrap();

    Ok(())
}

// 1. Make this thing async!
// 2. List namespaces as root level directories.
