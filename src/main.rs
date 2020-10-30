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
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

const TTL: Duration = Duration::from_secs(1); // 1 second

type Inode = u64;

type KubernetesFilesystem = BTreeMap<Inode, KubernetesResource>;

#[derive(Serialize, Deserialize, Copy, Clone, PartialEq, Hash)]
enum FileKind {
    File,
    Directory,
    Symlink,
}

#[derive(Hash, Clone, Debug, PartialEq)]
struct KubernetesResource {
    // Name of the resource
    name: String,

    // If this resource contains others (for now this is a Namespace/directory)
    subresources: Option<Vec<Inode>>,

    // file_type will depend on the value of subresource (some or none), but for
    // now we'll keep it.
    file_type: FileType,
}

impl KubernetesResource {
    fn new_namespace(name: &str) -> KubernetesResource {
        KubernetesResource {
            name: name.into(),
            subresources: None,
            file_type: FileType::Directory,
        }
    }

    fn new_pod(name: &str) -> KubernetesResource {
        KubernetesResource {
            name: name.into(),
            subresources: None,
            file_type: FileType::RegularFile,
        }
    }

    fn inode(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);

        s.finish()
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
            kind: self.file_type,
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
    filesystem: KubernetesFilesystem,
    // TODO: add some kind of cache.. or is TTL going to be enough?
    // cache: bool,
}

impl KubeFS {
    fn new() -> Self {
        let mut kf = KubeFS {
            // `directory` has the top-level directories (namespaces)
            filesystem: BTreeMap::new(),
            // always fetch from kube-api
            // cache: false,
        };

        kf.populate_root_filesystem();
        kf
    }

    fn populate_namespace_filesystem(&mut self, namespace_inode: Inode) {
        let mut namespace_resource = self.filesystem.get(&namespace_inode).unwrap().clone();

        // we'll remove the current object
        self.filesystem.remove(&namespace_inode).unwrap();

        // For now we only have Pods
        let pods = get_pods(&namespace_resource.name).expect("Could nto read pods from cluster");
        let mut subresources = vec![];
        for pod in pods {
            let kr = KubernetesResource::new_pod(&pod);
            let inode = kr.inode();
            self.filesystem.insert(inode, kr);
            subresources.push(inode);
        }

        namespace_resource.subresources = Some(subresources);
        self.filesystem.insert(namespace_inode, namespace_resource);
    }

    fn populate_root_filesystem(&mut self) {
        let namespaces = get_namespaces().expect("Could not read namespaces from cluster");
        let mut subresources = vec![];
        for namespace in namespaces {
            // Adds all the directories
            let kr = KubernetesResource::new_namespace(&namespace);
            let inode = kr.inode();
            self.filesystem.insert(inode, kr);
            subresources.push(inode);
        }

        let root = KubernetesResource {
            name: "/".into(),
            subresources: Some(subresources),
            file_type: FileType::Directory,
        };

        self.filesystem.insert(1u64, root);
    }
}

impl Filesystem for KubeFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("lookup");
        // These are file-like objects at the root /
        println!(
            "Looking up: {}, with parent {}",
            name.to_str().unwrap(),
            parent
        );

        // TODO: We need to find the node that corresponds to 'parent'. This one is easy, it requieres
        // self.filesystem[parent] and look at every children in this node.

        // println!("Directory has {} entries", self.directory.len());
        let sname: String = name.to_str().unwrap().into();

        if let Some(directory) = self.filesystem.get(&parent) {
            let subresources = directory.subresources.clone().unwrap();
            for resource_inode in &subresources {
                if let Some(resource) = self.filesystem.get(resource_inode) {
                    if resource.name == sname {
                        reply.entry(&TTL, &resource.file_attr(), 1);
                        break;
                    }
                }
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr for inode {}", ino);
        match self.filesystem.get(&ino) {
            Some(resource) => {
                println!("Found resource {}", resource.name);
                reply.attr(&TTL, &resource.file_attr())
            }
            None => {}
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
        println!("read for inode {} (offset {})", ino, offset);
        // if ino == 2 {
        //     reply.data(&HELLO_TXT_CONTENT.as_bytes()[offset as usize..]);
        // } else {
        //     reply.error(ENOENT);
        // }
        reply.data(&"Hello World!\n".as_bytes()[offset as usize..]);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        println!("readdir");
        let mut entries = vec![
            (ino, FileType::Directory, String::from(".")),
            (ino, FileType::Directory, String::from("..")),
        ];

        if ino != 1 {
            // not needed if root directory
            self.populate_namespace_filesystem(ino);
        }
        if let Some(directory) = self.filesystem.get(&ino) {
            let subresources = directory.subresources.clone().unwrap();
            for resource_inode in &subresources {
                if let Some(resource) = self.filesystem.get(resource_inode) {
                    entries.push((*resource_inode, resource.file_type, resource.name.clone()));
                } else {
                    println!(
                        "Could not find resource with inode {} in filesystem",
                        resource_inode
                    );
                }
            }
        } else {
            reply.error(ENOENT);
            return;
        }

        for (idx, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            reply.add(entry.0, (idx + 1) as i64, entry.1, &entry.2);
        }

        reply.ok();
    }

    fn init(&mut self, _req: &Request<'_>) -> Result<(), libc::c_int> {
        println!("init");
        Ok(())
    }

    fn destroy(&mut self, _req: &Request<'_>) {
        println!("destroy");
    }

    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {
        println!("forget");
    }

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
        println!("setattr");
        reply.error(libc::ENOSYS);
    }

    fn readlink(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyData) {
        println!("readlink");
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
        println!("mknod");
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
        println!("mkdir");
        reply.error(libc::ENOSYS);
    }

    fn unlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        println!("unlink");
        reply.error(libc::ENOSYS);
    }

    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: fuser::ReplyEmpty) {
        println!("rmdir");
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
        println!("symlink");
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
        println!("rename");
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
        println!("link");
        reply.error(libc::ENOSYS);
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: u32, reply: fuser::ReplyOpen) {
        println!("open");
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
        println!("write");
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
        println!("flush");
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
        println!("release");
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
        println!("fsync");
        reply.error(libc::ENOSYS);
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: u32, reply: fuser::ReplyOpen) {
        println!("opendir");
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
        println!("releasedir");
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
        println!("fsyncdir");
        reply.error(libc::ENOSYS);
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        println!("statfs");
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 512);
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
        println!("setxattr");
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
        println!("getxattr");
        reply.error(libc::ENOSYS);
    }

    fn listxattr(&mut self, _req: &Request<'_>, _ino: u64, _size: u32, reply: fuser::ReplyXattr) {
        println!("listxattr");
        reply.error(libc::ENOSYS);
    }

    fn removexattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        println!("removexattr");
        reply.error(libc::ENOSYS);
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: u32, reply: fuser::ReplyEmpty) {
        println!("access");
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
        println!("create");
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
        println!("getlk");
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
        println!("setlk");
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
        println!("bmap");
        reply.error(libc::ENOSYS);
    }

    #[cfg(target_os = "macos")]
    fn setvolname(&mut self, _req: &Request<'_>, _name: &OsStr, reply: fuser::ReplyEmpty) {
        println!("setvolname");
        reply.error(libc::ENOSYS);
    }

    #[cfg(target_os = "macos")]
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
        println!("exchange");
        reply.error(libc::ENOSYS);
    }

    #[cfg(target_os = "macos")]
    fn getxtimes(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyXTimes) {
        println!("getxtimes");
        reply.error(libc::ENOSYS);
    }
}

// Returns a list with the names of all namespaces.
#[tokio::main]
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
