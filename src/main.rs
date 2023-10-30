mod filesystem;
mod k8s_resource;
mod kubectl;

use env_logger::Env;
use filesystem::K8sFS;
use fuser::{self, MountOption};

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let fs = K8sFS::new();
    let mount_options = vec![MountOption::RO, MountOption::FSName(fs.name())];
    let mount_point = "mountpoint";

    log::info!("Mounting K8sFS...");
    let _ = fuser::mount2(fs, mount_point, &mount_options);
}
