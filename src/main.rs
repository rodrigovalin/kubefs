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
use k8s_openapi::api::core::v1::{Namespace, Pod};
use kube::Client;

use kube::api::{Api, ListParams, Meta};

// Used to Hash namespace/resource into u64
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

type Inode = u64;

type DirectoryDescriptor = BTreeMap<Inode, KubernetesResource>;
type NamespaceContents = BTreeMap<Inode, Vec<KubernetesResource>>;

#[derive(Serialize, Deserialize, Copy, Clone, PartialEq, Hash)]
enum FileKind {
    File,
    Directory,
    Symlink,
}

#[derive(Hash, Clone, Debug)]
struct KubernetesResource {
    reference: ResourceScope,
    kind: String,
    group: String,

    file_type: FileType,
}

#[derive(Hash, Clone, Debug)]
enum ResourceScope {
    Namespaced { name: String, namespace: String },
    Cluster { name: String },
}

impl KubernetesResource {
    fn new_namespace(name: &str) -> KubernetesResource {
        KubernetesResource {
            reference: ResourceScope::Cluster { name: name.into() },
            kind: "namespace".into(),
            group: "corev1".into(),
            file_type: FileType::Directory,
        }
    }
    fn inode(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);

        s.finish()
    }

    fn name(&self) -> String {
        match &self.reference {
            ResourceScope::Namespaced { name, namespace } => format!("{}/{}", namespace, name),
            ResourceScope::Cluster { name } => name.clone(),
        }
    }

    fn file_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.inode(),
            size: 1,
            blocks: 1,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH, // replace with time of creation of resource?
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o774, // rwxrwxr
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
            blksize: 512,
            padding: 0,
        }
    }
}

struct KubeFS {
    directory: DirectoryDescriptor,
    namespace_directory: NamespaceContents,
    cache: bool,
}

impl KubeFS {
    fn new() -> Self {
        let mut kf = KubeFS {
            // `directory` has the top-level directories (namespaces)
            directory: DirectoryDescriptor::new(),
            // `namespaced_directory` has a series of objects per namespace.
            namespace_directory: NamespaceContents::new(),

            // always fetch from kube-api
            cache: false,
        };
        kf.populate_directory_with_namespaces(
            get_namespaces_blocking().expect("Could not read namespaces from Kubernetes"),
        );

        kf
    }

    fn populate_directory_with_namespaces(&mut self, namespaces: Vec<String>) {
        for ns in &namespaces {
            let k = KubernetesResource::new_namespace(ns);
            self.directory.insert(k.inode(), k);
        }
    }

    fn populate_namespaces_directory(&mut self, inode: Inode) {
        let namespace_name = match self.directory.get(&inode) {
            Some(namespace) => match &namespace.reference {
                ResourceScope::Namespaced { name, namespace } => name,
                ResourceScope::Cluster { name } => name,
            },
            None => unimplemented!(),
        };

        let mut resources = vec![];
        let pods = get_pods(&namespace_name).expect("Error fetching Pods from kube-api");
        for pod in pods {
            resources.push(KubernetesResource {
                reference: ResourceScope::Namespaced {
                    name: pod,
                    namespace: namespace_name.into(),
                },
                kind: "Pod".into(),
                group: "corev1".into(),
                file_type: FileType::RegularFile,
            })
        }

        self.namespace_directory.insert(inode, resources);
    }

    // finds resources that belong to a namespace on the fly.
    fn find_namespaced_resources(&self, inode: Inode) -> Vec<KubernetesResource> {
        match self.namespace_directory.get(&inode) {
            Some(directory) => directory.to_vec(),
            None => vec![],
        }
    }
}

impl Filesystem for KubeFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        // These are file-like objects at the root /
        println!(
            "Looking up: {}, with parent {}",
            name.to_str().unwrap(),
            parent
        );

        println!("Directory has {} entries", self.directory.len());
        let sname: String = name.to_str().unwrap().into();
        if parent == 1 {
            println!("Traversing namespaces");

            for (_inode, resource) in self.directory.iter() {
                let name = match &resource.reference {
                    ResourceScope::Namespaced { name, namespace: _ } => name,
                    ResourceScope::Cluster { name } => name,
                };
                println!("Checking if {} matches what I'm looking for", &name);
                if *name == sname {
                    println!("{} matches!", name);
                    reply.entry(&TTL, &resource.file_attr(), 1);
                    break;
                }
            }
        } else {
            if !self.namespace_directory.contains_key(&parent) {
                self.populate_namespaces_directory(parent);
            }

            if let Some(resources) = self.namespace_directory.get(&parent) {
                for resource in resources {
                    let name = match &resource.reference {
                        ResourceScope::Namespaced { name, namespace: _ } => name,
                        ResourceScope::Cluster { name } => name,
                    };
                    println!("Checking if {} matches what I'm looking for", &name);
                    if *name == sname {
                        println!("{} matches!", name);
                        reply.entry(&TTL, &resource.file_attr(), 1);
                        break;
                    }
                }
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr {}", ino);
        match ino {
            1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
            2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
            _ => reply.attr(&TTL, &HELLO_DIR_ATTR),
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
        let mut entries = vec![
            (ino, FileType::Directory, String::from(".")),
            (ino, FileType::Directory, String::from("..")),
        ];

        if ino == 1 {
            // Parent directory (root) only has a bunch of namespaces
            for (inode, resource) in &self.directory {
                println!("This is {:?}", resource);
                let name = match &resource.reference {
                    ResourceScope::Namespaced { name, namespace: _ } => name,
                    ResourceScope::Cluster { name } => name,
                };
                entries.push((*inode, FileType::Directory, name.into()));
            }
        } else {
            println!("Checking inode {}", ino);
            // this should cache the contents for a while, but not yet
            if !self.cache {
                self.populate_namespaces_directory(ino);
            } else {
                unimplemented!();
            }
            let namespaced_resources = self.find_namespaced_resources(ino);
            for nsr in namespaced_resources {
                let name: String = match &nsr.reference {
                    ResourceScope::Namespaced { name, namespace: _ } => name.clone(),
                    ResourceScope::Cluster { name } => name.clone(),
                };
                entries.push((nsr.inode(), nsr.file_type, name));
            }
        }

        for (idx, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            reply.add(entry.0, (idx + 1) as i64, entry.1, &entry.2);
        }

        reply.ok();
    }

    fn init(&mut self, _req: &Request<'_>) -> Result<(), libc::c_int> {
        Ok(())
    }

    fn destroy(&mut self, _req: &Request<'_>) {}

    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {}

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<std::time::SystemTime>,
        _atime_now: bool,
        _mtime: Option<std::time::SystemTime>,
        _mtime_now: bool,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn readlink(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyData) {
        reply.error(libc::ENOSYS);
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        reply: ReplyEntry,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn unlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: fuser::ReplyEmpty) {
        reply.error(libc::ENOSYS);
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: u32, reply: fuser::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _flags: u32,
        reply: fuser::ReplyWrite,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: u32, reply: fuser::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _size: u32,
        reply: fuser::ReplyXattr,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn listxattr(&mut self, _req: &Request<'_>, _ino: u64, _size: u32, reply: fuser::ReplyXattr) {
        reply.error(libc::ENOSYS);
    }

    fn removexattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: u32, reply: fuser::ReplyEmpty) {
        reply.error(libc::ENOSYS);
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _flags: u32,
        reply: fuser::ReplyCreate,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn getlk(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        reply: fuser::ReplyLock,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn setlk(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        _sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn bmap(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _blocksize: u32,
        _idx: u64,
        reply: fuser::ReplyBmap,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn setvolname(&mut self, _req: &Request<'_>, _name: &OsStr, reply: fuser::ReplyEmpty) {
        reply.error(libc::ENOSYS);
    }

    fn exchange(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        _options: u64,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn getxtimes(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyXTimes) {
        reply.error(libc::ENOSYS);
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

// Returns a list with the names of all namespaces.
#[tokio::main]
async fn get_pods(namespace: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let client = Client::try_default().await?;

    let pod_api: Api<Pod> = Api::namespaced(client, namespace);
    let lp = ListParams::default();
    let mut pod_names: Vec<String> = vec![];

    for namespace in pod_api.list(&lp).await? {
        pod_names.push(String::from(Meta::name(&namespace)));
    }

    Ok(pod_names)
}

fn main() -> Result<(), Box<dyn Error>> {
    let mountpoint = env::args_os().nth(1).unwrap();
    let options = vec![
        MountOption::RO,
        MountOption::FSName("hello".to_string()),
        MountOption::AutoUnmount,
    ];

    fuser::mount2(KubeFS::new(), mountpoint, &options).unwrap();

    Ok(())
}

// Each file-like object needs to be:
//
// 1. If it is a directory, readdir should returns its contents. It is indexed by inode.
//    This will happen everytime we list a directory. This can be the root (inode == 1) or
//    a namespace, with an 'unknown' inode.
//
// 2. We'll call lookup on each file-like object, and it needs to return a list of files on it.
//    File is requested by name, being part of parent, which is an inode'd directory. This means that
//    we need a way of storing the
