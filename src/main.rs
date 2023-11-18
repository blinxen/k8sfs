mod filesystem;
mod k8s_resource;
mod kubectl;

use clap::{Arg, ArgAction, Command};
use env_logger::Env;
use filesystem::K8sFS;
use fuser::{self, MountOption};

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let matches = Command::new("k8sfs")
        .version("0.1.0")
        .author("blinxen")
        .arg(
            Arg::new("mountpoint")
                .required(true)
                .index(1)
                .help("Filesystem mount point"),
        )
        .arg(
            Arg::new("allow-write")
                .long("allow-write")
                .short('w')
                .action(ArgAction::SetTrue)
                .help(
                    "Allow writing to filesystem.\nThis means that users can create kubernetes resources with IO operations.",
                ),
        )
        .get_matches();

    let fs = K8sFS::new();

    let mut mount_options = vec![MountOption::FSName(fs.name())];
    if matches.get_flag("allow-write") {
        mount_options.push(MountOption::RW);
    } else {
        mount_options.push(MountOption::RO);
    }

    log::info!("Mounting K8sFS...");
    fuser::mount2(
        fs,
        matches.get_one::<String>("mountpoint").unwrap(),
        &mount_options,
    )
    .expect("Unexpected error when exiting the filesystem");
}
