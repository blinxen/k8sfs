use crate::k8s_resource::{ResourceFile, ResourceType};
use crate::kubectl;
use fuser::{self, Filesystem, ReplyAttr, ReplyDirectory, ReplyEntry, Request};
// https://www2.hs-fulda.de/~klingebiel/c-stdlib/sys.errno.h.htm
use libc::{ENOBUFS, ENOENT};
use std::cmp::min;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io::Read;
use std::time::Duration;

const TTL: Duration = Duration::from_secs(1);
pub type Inode = u64;
pub type Offset = i64;
// Tuple values explanations:
//   * Resource: Contains the file type and k8s information that is associated with this Inode
//   * Vec<Inode>: Contains inodes for all children. This depends on the ResourceType.
//      * Context will contain all namespaces as directories
//      * Namespace will contain all deployments as directories
//      * Deployments will contain all pods as directories
//      * Pods will contain all containers as files
//      * Container --> Empty as containers don't have children
//   * Inode: Parent Inode
pub type File = (ResourceFile, Vec<Inode>);

pub struct K8sFS {
    // There is no specific reason why we chose BTreeMap as the inode table data structure
    // It was used in one of the fuser examples
    inode_table: BTreeMap<Inode, File>,
    next_inode: Inode,
}

// TODO: build_namespace_resource and build_pod_resource are very similiar
// they should be simplified
impl K8sFS {
    pub fn new() -> Self {
        K8sFS {
            inode_table: BTreeMap::new(),
            next_inode: 0,
        }
    }

    fn initialize_inode_table(&mut self) {
        log::info!("Initializing inode table");
        // Init FS root
        let root_inode = self.calculate_next_inode();
        let root = ResourceFile::new(
            root_inode,
            root_inode,
            String::from("root"),
            ResourceType::Root,
            String::from(""),
        );
        // Init kubernetes context (which is the kubernetes root)
        let k8s_context = ResourceFile::new(
            self.calculate_next_inode(),
            root.inode,
            kubectl::current_context(),
            ResourceType::Context,
            String::from(""),
        );
        self.inode_table
            .insert(root.inode, (root, vec![k8s_context.inode]));
        // Init kubernetes namespaces
        let mut namespaces: Vec<Inode> = Vec::new();
        for namespace in kubectl::namespaces() {
            let ns_inode = self.build_namespace_resource(namespace, &k8s_context);
            namespaces.push(ns_inode);
        }
        self.inode_table
            .insert(k8s_context.inode, (k8s_context, namespaces));
    }

    fn build_namespace_resource(&mut self, name: String, context: &ResourceFile) -> Inode {
        let namespace_inode = self.calculate_next_inode();
        let namespace = ResourceFile::new(
            namespace_inode,
            context.inode,
            name.clone(),
            ResourceType::Namespace,
            String::new(),
        );
        let namespace_definiton = ResourceFile::new(
            self.calculate_next_inode(),
            context.inode,
            name.clone() + "_definition.yaml",
            ResourceType::ResourceDefinition,
            format!(
                "kubectl --context {} describe namespaces {}",
                context.name, name
            ),
        );
        // Init kubernetes pods
        let mut pods = Vec::<Inode>::new();
        pods.push(namespace_definiton.inode);
        // Init kubernetes pods
        for pod in kubectl::pods(&namespace.name) {
            let pod_inode = self.build_pod_resource(pod, context, &namespace);
            pods.push(pod_inode);
        }
        self.inode_table.insert(namespace_inode, (namespace, pods));
        self.inode_table.insert(
            namespace_definiton.inode,
            (namespace_definiton, Vec::<Inode>::new()),
        );

        namespace_inode
    }

    fn build_pod_resource(
        &mut self,
        name: String,
        context: &ResourceFile,
        namespace: &ResourceFile,
    ) -> Inode {
        let pod_inode = self.calculate_next_inode();
        let mut pod_children = Vec::<Inode>::new();
        let pod = ResourceFile::new(
            pod_inode,
            namespace.inode,
            name.clone(),
            ResourceType::Pod,
            String::new(),
        );
        // Add a pod definition file for each pod
        let pod_definiton = ResourceFile::new(
            self.calculate_next_inode(),
            namespace.inode,
            name.clone() + "_definition.yaml",
            ResourceType::ResourceDefinition,
            format!(
                "kubectl --context {} --namespace {} describe pods {}",
                context.name, namespace.name, name
            ),
        );
        pod_children.push(pod_definiton.inode);

        self.inode_table.insert(pod_inode, (pod, pod_children));

        self.inode_table
            .insert(pod_definiton.inode, (pod_definiton, Vec::<Inode>::new()));

        pod_inode
    }

    pub fn name(&self) -> String {
        String::from("KubernetesFS")
    }

    fn get_file_by_name(&self, name: &OsStr, parent_inode: Inode) -> Option<&ResourceFile> {
        log::debug!(
            "Trying to search for {:?} with parent inode {} ",
            name,
            parent_inode
        );
        let mut file = None;
        if let Some((_, children)) = self.inode_table.get(&parent_inode) {
            for child in children.iter() {
                if let Some((found_file, _)) = self.inode_table.get(child) {
                    if found_file.name == name.to_string_lossy() {
                        log::debug!("Found {:?} with inode {}", name, found_file.inode);
                        file = Some(found_file);
                        break;
                    }
                } else {
                    log::error!(
                        "Child of {} could not be found in the inode table",
                        parent_inode
                    );
                }
            }
        } else {
            log::error!("Could not find parent with inode {}", parent_inode);
        }

        if file.is_none() {
            log::error!("Could not find file or directory with name {:?}", name);
        }

        file
    }

    fn get_file_by_inode(&self, inode: Inode) -> Option<&ResourceFile> {
        log::debug!(r#"Trying to search for file with inode "{}""#, inode);
        let mut file = None;

        if let Some((found_file, _)) = self.inode_table.get(&inode) {
            file = Some(found_file);
        } else {
            log::error!("Could not find file or directory with inode {}", inode);
        }

        file
    }

    fn calculate_next_inode(&mut self) -> Inode {
        let inode = self.next_inode;
        self.next_inode += 1;

        inode
    }
}

impl Filesystem for K8sFS {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        self.initialize_inode_table();
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: Inode, name: &OsStr, reply: ReplyEntry) {
        log::debug!(r#"Searching for file with the name "{:?}""#, name);

        // We could check access here or do other checks

        if let Some(file) = self.get_file_by_name(name, parent) {
            reply.entry(&TTL, &file.fileattrs(), 0);
        } else {
            reply.error(ENOENT);
        }
    }
    fn getattr(&mut self, _req: &Request, inode: Inode, reply: ReplyAttr) {
        log::debug!("Getting attributes for file with inode {}", inode);

        if let Some(file) = self.get_file_by_inode(inode) {
            reply.attr(&TTL, &file.fileattrs());
        } else {
            reply.error(ENOENT);
        }
    }

    // TODO: Allow creating namespaces
    // fn mkdir(
    //     &mut self,
    //     _req: &Request<'_>,
    //     parent: u64,
    //     name: &OsStr,
    //     mode: u32,
    //     umask: u32,
    //     reply: ReplyEntry,
    // ) {
    // }

    // TODO: Allow deleting namespace
    // fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {}

    // TODO: Allow renaming a kubernetes resource
    // fn rename(
    //     &mut self,
    //     _req: &Request<'_>,
    //     parent: u64,
    //     name: &OsStr,
    //     newparent: u64,
    //     newname: &OsStr,
    //     flags: u32,
    //     reply: fuser::ReplyEmpty,
    // ) {
    // }

    fn read(
        &mut self,
        _req: &Request<'_>,
        inode: Inode,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        log::debug!("Trying to read {}", inode);

        if let Some(file) = self.get_file_by_inode(inode) {
            // We must not read more than size
            // We should either read size or the file size if it is actually smaller
            let read_size = min(size as u64, file.size().saturating_sub(offset as u64));
            reply.data(
                file.get_desc()[offset as usize..]
                    .take(read_size)
                    .into_inner(),
            );
        } else {
            reply.error(ENOENT);
        }
    }

    // TODO: Allow updating a pods (basically kubectl edit)
    // fn write(
    //     &mut self,
    //     _req: &Request<'_>,
    //     ino: u64,
    //     fh: u64,
    //     offset: i64,
    //     data: &[u8],
    //     write_flags: u32,
    //     flags: i32,
    //     lock_owner: Option<u64>,
    //     reply: fuser::ReplyWrite,
    // ) {
    // }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        inode: Inode,
        _fh: u64,
        offset: Offset,
        mut reply: ReplyDirectory,
    ) {
        log::debug!("Listing directory for {}", inode);
        // Boolean value that tracks whether the reply buffer is full or not
        let mut buffer_full = false;

        if let Some((_, children)) = self.inode_table.get(&inode) {
            // See https://github.com/cberner/fuser/issues/267#issuecomment-1794405706
            for (index, child_inode) in children.iter().enumerate().skip(offset as usize) {
                if let Some((child_resource, _)) = self.inode_table.get(child_inode) {
                    log::debug!("Adding {} to reply buffer", child_resource.name);
                    if reply.add(
                        child_resource.inode,
                        offset + index as i64 + 1,
                        child_resource.filetype(),
                        OsStr::new(&child_resource.name),
                    ) {
                        log::error!(
                            "Reply buffer is full!!\nCould not add {}.\nThis should never happen!!",
                            child_resource.name
                        );
                        buffer_full = true;
                        break;
                    }
                } else {
                    log::error!("Could not find {} in the inode table", child_inode);
                }
            }
        } else {
            log::error!("Could not find {} in the inode table", inode);
        }

        if buffer_full {
            reply.error(ENOBUFS);
        } else {
            reply.ok();
        }
    }

    // TODO: Allow creating pods
    // fn create(
    //     &mut self,
    //     _req: &Request<'_>,
    //     parent: u64,
    //     name: &OsStr,
    //     mode: u32,
    //     umask: u32,
    //     flags: i32,
    //     reply: fuser::ReplyCreate,
    // ) {
    // }
}
