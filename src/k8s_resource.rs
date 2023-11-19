use crate::filesystem::Inode;
use fuser::{FileAttr, FileType};
use std::{process::Command, time::SystemTime};

// Block size is the amount of bytes that can be requests during read / write IO operation
const BLOCK_SIZE: u32 = 1024;

//  Resource types that are currently supported
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ResourceType {
    Root,
    Context,
    Namespace,
    Pod,
    ResourceDefinition,
}

fn build_kubectl_command(
    action: &str,
    resource_type: ResourceType,
    context: &str,
    namespace: &str,
    resource_name: &str,
) -> String {
    match resource_type {
        ResourceType::Namespace => format!(
            "kubectl --context {} {} namespaces {}",
            context, action, resource_name
        ),
        ResourceType::Pod => format!(
            "kubectl --context {} --namespace {} {} pods {}",
            context, namespace, action, resource_name
        ),
        _ => format!(
            "Files of type {:?} do not support {} !",
            resource_type, action
        ),
    }
}

// Represents a kubernetes resource
pub struct ResourceFile {
    pub inode: Inode,
    pub parent: Inode,
    resource_type: ResourceType,
    pub name: String,
    delete_cmd: String,
    description_cmd: String,
}

impl ResourceFile {
    pub fn new(
        inode: Inode,
        parent: Inode,
        resource_name: &str,
        resource_type: ResourceType,
        context: &str,
        namespace: &str,
    ) -> Self {
        Self {
            inode,
            parent,
            resource_type,
            name: resource_name.to_string(),
            delete_cmd: build_kubectl_command(
                "delete",
                resource_type,
                context,
                namespace,
                resource_name,
            ),
            description_cmd: build_kubectl_command(
                "describe",
                resource_type,
                context,
                namespace,
                resource_name,
            ),
        }
    }

    pub fn filetype(&self) -> FileType {
        match self.resource_type {
            // Filesystem root
            ResourceType::Root => FileType::Directory,
            ResourceType::Context => FileType::Directory,
            ResourceType::Namespace => FileType::Directory,
            ResourceType::Pod => FileType::Directory,
            ResourceType::ResourceDefinition => FileType::RegularFile,
        }
    }

    pub fn fileattrs(&self) -> FileAttr {
        let permissions = if self.filetype() == FileType::Directory {
            0o555
        } else {
            0o444
        };
        let file_size = self.size();
        let file_block_size = if file_size > 0 {
            (file_size + BLOCK_SIZE as u64 - 1) / file_size
        } else {
            0
        };

        FileAttr {
            ino: self.inode,
            // Length is in bytes so getting the Vec length should be equivaled to the file size
            size: file_size,
            // We add a whole block and subtract 1 to catch all cases where the file
            // size is less than a single block
            blocks: file_block_size,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            kind: self.filetype(),
            perm: permissions,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }

    pub fn get_desc(&self) -> Vec<u8> {
        if self.filetype() != FileType::RegularFile {
            log::error!("Fatal ERROR!! You should never reach this!!");
            return Vec::new();
        }

        let command: Vec<&str> = self.description_cmd.split(' ').collect();
        let command_args = &command[1..];
        let description = Command::new(command[0]).args(command_args).output();

        if let Ok(description) = description {
            if description.status.success() {
                description.stdout
            } else {
                log::error!("Could not get description for {}", self.name);
                log::debug!(
                    "Command failed with: {}",
                    String::from_utf8(description.stderr)
                        .unwrap_or(String::from("Could not parse stderr! Invalid UTF-8!"))
                );
                Vec::new()
            }
        } else {
            log::error!("Could not get description for {}", self.name);
            log::debug!("Comand failed with: {:?}", description.err());
            Vec::new()
        }
    }

    pub fn size(&self) -> u64 {
        if self.filetype() == FileType::RegularFile {
            self.get_desc().len() as u64
        } else {
            0
        }
    }
}
