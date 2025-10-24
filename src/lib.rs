use anyhow::{self, Ok};
use raster::EDF;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Output};

pub struct PodmanCtx {
    pub podman_path: PathBuf,
    pub module: Option<String>,
    pub graphroot: Option<PathBuf>,
    pub runroot: Option<PathBuf>,
    pub parallax_mount_program: Option<PathBuf>,
    pub ro_store: Option<PathBuf>,
}

pub struct ContainerCtx {
    pub name: String,
    pub interactive: bool,
    pub detach: bool,
    pub set_env: bool,
    pub pidfile: Option<PathBuf>,
}

fn base_command(podman_ctx: Option<&PodmanCtx>) -> Command {
    let Some(ctx) = podman_ctx else {
        return Command::new("podman");
    };

    let mut cmd = Command::new(ctx.podman_path.as_path());
    cli_opt(
        &mut cmd,
        "--root",
        ctx.graphroot.as_deref().map(Path::as_os_str),
    );
    cli_opt(
        &mut cmd,
        "--runroot",
        ctx.runroot.as_deref().map(Path::as_os_str),
    );

    cmd
}

fn run_command(podman_ctx: Option<&PodmanCtx>) -> Command {
    let mut cmd = base_command(podman_ctx);

    if let Some(ctx) = podman_ctx {
        cli_opt(&mut cmd, "--module", ctx.module.as_deref().map(OsStr::new));
        cli_storage_opt(
            &mut cmd,
            "additionalimagestore",
            ctx.ro_store.as_deref().map(Path::as_os_str),
        );
        cli_storage_opt(
            &mut cmd,
            "mount_program",
            ctx.parallax_mount_program.as_deref().map(Path::as_os_str),
        );
    }

    cmd.arg("run");
    cmd
}

pub fn run<I, S>(args: I, podman_ctx: Option<&PodmanCtx>) -> ExitStatus
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_command(podman_ctx)
        .args(args)
        .status()
        .expect("Failed to execute command")
}

pub fn run_output<I, S>(args: I, podman_ctx: Option<&PodmanCtx>) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_command(podman_ctx)
        .args(args)
        .output()
        .expect("Failed to execute command")
}

fn run_from_edf_command<I, S>(
    edf: &EDF,
    p_ctx: Option<&PodmanCtx>,
    c_ctx: &ContainerCtx,
    container_cmd: I,
) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut cmd = run_command(p_ctx);

    cmd.arg("--rm");
    cli_flag(&mut cmd, c_ctx.detach, "--detach");
    cli_flag(&mut cmd, c_ctx.interactive, "-it");
    cli_flag(&mut cmd, !edf.writable, "--read-only");

    cli_opt(&mut cmd, "--name", Some(OsStr::new(&c_ctx.name)));
    cli_opt(
        &mut cmd,
        "--pidfile",
        c_ctx.pidfile.as_deref().map(Path::as_os_str),
    );

    //TODO: support entrypoint redefinition as well
    cli_flag(&mut cmd, !edf.entrypoint, "--entrypoint=");

    if !edf.workdir.is_empty() {
        cli_opt(&mut cmd, "--workdir", Some(OsStr::new(&edf.workdir)));
    }
    for mnt in &edf.mounts {
        cli_opt(
            &mut cmd,
            "--volume",
            Some(OsStr::new(&mnt.to_volume_string())),
        );
    }
    for dev in &edf.devices {
        cli_opt(&mut cmd, "--device", Some(OsStr::new(dev)));
    }
    for (key, val) in &edf.env {
        cli_kv(&mut cmd, "--env", OsStr::new(key), OsStr::new(val));
    }
    for (key, val) in &edf.annotations {
        cli_kv(&mut cmd, "--annotation", OsStr::new(key), OsStr::new(val));
    }

    cmd.arg(&edf.image);
    cmd.args(container_cmd);

    cmd
}

pub fn run_from_edf<I, S>(
    edf: &EDF,
    p_ctx: Option<&PodmanCtx>,
    c_ctx: &ContainerCtx,
    container_cmd: I,
) -> ExitStatus
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_from_edf_command(edf, p_ctx, c_ctx, container_cmd)
        .status()
        .expect("Failed to execute command")
}

pub fn run_from_edf_output<I, S>(
    edf: &EDF,
    p_ctx: Option<&PodmanCtx>,
    c_ctx: &ContainerCtx,
    container_cmd: I,
) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_from_edf_command(edf, p_ctx, c_ctx, container_cmd)
        .output()
        .expect("Failed to execute command")
}

pub fn pull(image: &str, podman_ctx: Option<&PodmanCtx>) {
    base_command(podman_ctx)
        .args(["pull", image])
        .output()
        .expect("Failed to execute command");
}

pub fn rmi(image: &str, podman_ctx: Option<&PodmanCtx>) {
    let mut cmd = base_command(podman_ctx);

    if let Some(ctx) = podman_ctx {
        cli_storage_opt(
            &mut cmd,
            "additionalimagestore",
            ctx.ro_store.as_deref().map(Path::as_os_str),
        );
    }

    cmd.args(["rmi", image])
        .output()
        .expect("Failed to execute command");
}

pub fn rm(name: &str, podman_ctx: Option<&PodmanCtx>) {
    let mut cmd = base_command(podman_ctx);

    if let Some(ctx) = podman_ctx {
        cli_storage_opt(
            &mut cmd,
            "additionalimagestore",
            ctx.ro_store.as_deref().map(Path::as_os_str),
        );
    }

    cmd.args(["rm", name])
        .output()
        .expect("Failed to execute command");
}

pub fn images(podman_ctx: Option<&PodmanCtx>) {
    let mut cmd = base_command(podman_ctx);

    if let Some(ctx) = podman_ctx {
        cli_storage_opt(
            &mut cmd,
            "additionalimagestore",
            ctx.ro_store.as_deref().map(Path::as_os_str),
        );
    }

    cmd.arg("images");
    cmd.status().expect("Failed to execute command");
}

pub fn image_exists(image: &str, podman_ctx: Option<&PodmanCtx>) -> bool {
    let mut cmd = base_command(podman_ctx);

    if let Some(ctx) = podman_ctx {
        cli_storage_opt(
            &mut cmd,
            "additionalimagestore",
            ctx.ro_store.as_deref().map(Path::as_os_str),
        );
    }

    cmd.args(["image", "exists", image]);
    cmd.status().expect("Failed to execute command").success()
}

pub fn inspect(target: &str, format: Option<&str>, podman_ctx: Option<&PodmanCtx>) -> Output {
    let mut cmd = base_command(podman_ctx);

    if let Some(ctx) = podman_ctx {
        cli_storage_opt(
            &mut cmd,
            "additionalimagestore",
            ctx.ro_store.as_deref().map(Path::as_os_str),
        );
    }

    cmd.args(["--log-level=error", "inspect"]);

    if let Some(fmt) = format {
        cmd.args(["-f", fmt]);
    }

    cmd.arg(target);
    cmd.output().expect("Failed to execute command")
}

pub fn info(format: Option<&str>, podman_ctx: Option<&PodmanCtx>) -> Output {
    let mut cmd = base_command(podman_ctx);
    cmd.arg("info");

    if let Some(fmt) = format {
        cmd.args(["-f", fmt]);
    }

    cmd.output().expect("Failed to execute command")
}

pub fn version(module: Option<&str>) -> Output {
    let mut cmd = base_command(None);
    cli_opt(&mut cmd, "--module", module.map(OsStr::new));

    cmd.arg("version")
        .output()
        .expect("Failed to execute command")
}

// Note: Podman yields `0` for stopped containers
pub fn get_container_pid(name: &str, podman_ctx: Option<&PodmanCtx>) -> anyhow::Result<u32> {
    let output = inspect(name, Some("{{.State.Pid}}"), podman_ctx);

    if !output.status.success() {
        // include stderr to make debugging nicer
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("podman inspect failed: {}", stderr.trim());
    }

    // Podman prints a line like "12345\n"
    let s = str::from_utf8(&output.stdout)?;
    let s = s.trim(); // drop newline/whitespace

    let pid: u32 = s.parse()?;
    Ok(pid)
}

// Retrieves the pid of a running container from the default pidfile for an overlay store driver
// If the runroot is passed as argument, this function is much faster than get_container_pid(),
// which uses `podman inspect`.
// This function does not work if:
//   - the container is stopped
//   - a custom pidfile was specified in `podman run`
//   - storage driver is not overlay
pub fn get_container_pid_from_default_file(
    container_id: &str,
    runroot: Option<&PathBuf>,
) -> anyhow::Result<u32> {
    let mut cnt_pidfile = PathBuf::new();

    if let Some(rr) = runroot {
        cnt_pidfile.push(rr);
    } else {
        // If we weren't given a runroot as argument, retrieve it from `podman info`
        // Notice that here we pass None as podman context: if a specific podman context were
        // to be passed to this function just to propagate the runroot, then the caller could
        // have provided the runroot directly by passing the related PodmanCtx field
        let runroot = info(Some("{{.Store.RunRoot}}"), None);
        let runroot = str::from_utf8(&runroot.stdout)?;
        let runroot = runroot.trim();
        cnt_pidfile.push(runroot);
    }

    cnt_pidfile.push("overlay-containers");
    cnt_pidfile.push(container_id);
    cnt_pidfile.push("userdata/pidfile");
    let mut cnt_pidfile = File::open(cnt_pidfile)?;

    let mut pid = String::new();
    cnt_pidfile.read_to_string(&mut pid)?;
    let pid: u32 = pid.parse()?;
    Ok(pid)
}

fn parallax_command(
    parallax_path: &PathBuf,
    podman_ctx: &PodmanCtx,
    image: &str,
    action: &str,
) -> Command {
    let mut cmd = Command::new(parallax_path);

    cmd.arg("--podmanRoot")
        .arg(
            podman_ctx
                .graphroot
                .as_ref()
                .expect("Missing graphroot in parallax_migrate()"),
        )
        .arg("--roStoragePath")
        .arg(
            &podman_ctx
                .ro_store
                .as_ref()
                .expect("Missing read-only store path in parallax_migrate()"),
        );

    cmd.arg(format!("--{action}")).arg("--image").arg(image);
    cmd
}

fn parallax_execute_command(
    parallax_path: &PathBuf,
    podman_ctx: &PodmanCtx,
    image: &str,
    action: &str,
) -> anyhow::Result<()> {
    let output = parallax_command(parallax_path, podman_ctx, image, action)
        .output()
        .expect(&format!("Failed to execute `parallax {action}`"));

    if !output.status.success() {
        // include stderr to make debugging nicer
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("parallax {action} failed: {}", stderr.trim());
    }
    Ok(())
}

pub fn parallax_migrate(
    parallax_path: &PathBuf,
    podman_ctx: &PodmanCtx,
    image: &str,
) -> anyhow::Result<()> {
    parallax_execute_command(parallax_path, podman_ctx, image, "migrate")
}

pub fn parallax_rmi(
    parallax_path: &PathBuf,
    podman_ctx: &PodmanCtx,
    image: &str,
) -> anyhow::Result<()> {
    parallax_execute_command(parallax_path, podman_ctx, image, "rmi")
}

fn cli_flag(cmd: &mut Command, on: bool, name: &str) {
    if on {
        cmd.arg(name);
    }
}

//TODO: Consider using AsRef<OsStr> to streamline passing of val
fn cli_opt(cmd: &mut Command, name: &str, val: Option<&OsStr>) {
    if let Some(v) = val {
        cmd.arg(name);
        cmd.arg(v);
    }
}

//TODO: Consider using AsRef<OsStr> to streamline passing of val
fn cli_storage_opt(cmd: &mut Command, name: &str, val: Option<&OsStr>) {
    if let Some(v) = val {
        cli_kv(cmd, "--storage-opt", OsStr::new(name), v);
    }
}

//TODO: Consider using AsRef<OsStr> to streamline passing of key and val
fn cli_kv(cmd: &mut Command, name: &str, key: &OsStr, val: &OsStr) {
    cmd.arg(name);
    cmd.arg(os_string_key_val(key, val));
}

// Build "<name>=<val>" without assuming UTF-8
fn os_string_key_val(key: &OsStr, val: &OsStr) -> OsString {
    let mut buf = OsString::with_capacity(key.len() + 1 + val.len());
    buf.push(key);
    buf.push(OsStr::new("="));
    buf.push(val);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use raster;

    #[test]
    fn test_run_from_edf_command() {
        let p_ctx = PodmanCtx {
            podman_path: PathBuf::from("/usr/bin/podman"),
            module: Some(String::from("hpc")),
            graphroot: Some(PathBuf::from("/dev/shm/sarus-test/graphroot")),
            runroot: Some(PathBuf::from("/dev/shm/sarus-test/runroot")),
            parallax_mount_program: Some(PathBuf::from(
                "/usr/local/sarus-test/parallax_mount_program",
            )),
            ro_store: Some(PathBuf::from("/scratch/user/parallax/store")),
        };

        let c_ctx = ContainerCtx {
            name: String::from("edf_test"),
            interactive: true,
            detach: true,
            set_env: true,
            pidfile: Some(PathBuf::from("/tmp/test/pidfile")),
        };

        let edf_path = std::env::current_dir()
            .unwrap()
            .join("tests/edf/run_from_edf_test.toml");
        let edf =
            raster::render(edf_path.to_string_lossy().into_owned()).expect("Failed rendering EDF");

        let cmd = run_from_edf_command(&edf, Some(&p_ctx), &c_ctx, ["bash"]);
        assert_eq!(cmd.get_program(), OsStr::new("/usr/bin/podman"));

        let args: Vec<&OsStr> = cmd.get_args().collect();
        assert_eq!(args.len(), 40);

        let args_head: Vec<&OsStr> = vec![
            OsStr::new("--root"),
            OsStr::new("/dev/shm/sarus-test/graphroot"),
            OsStr::new("--runroot"),
            OsStr::new("/dev/shm/sarus-test/runroot"),
            OsStr::new("--module"),
            OsStr::new("hpc"),
            OsStr::new("--storage-opt"),
            OsStr::new("additionalimagestore=/scratch/user/parallax/store"),
            OsStr::new("--storage-opt"),
            OsStr::new("mount_program=/usr/local/sarus-test/parallax_mount_program"),
            OsStr::new("run"),
            OsStr::new("--rm"),
            OsStr::new("--detach"),
            OsStr::new("-it"),
            OsStr::new("--read-only"),
            OsStr::new("--name"),
            OsStr::new("edf_test"),
            OsStr::new("--pidfile"),
            OsStr::new("/tmp/test/pidfile"),
            OsStr::new("--entrypoint="),
        ];
        assert_eq!(args[..20], args_head);

        // Use any() and iterator windows to be flexible w.r.t HashMap ordering and
        // at the same time check that option/value pairs are respected
        assert!(
            args.windows(2)
                .any(|w| w == [OsStr::new("--workdir"), OsStr::new("/develop")])
        );
        assert!(args.windows(2).any(|w| w
            == [
                OsStr::new("--volume"),
                OsStr::new("/home/user/test:/develop")
            ]));
        assert!(
            args.windows(2)
                .any(|w| w == [OsStr::new("--volume"), OsStr::new("/src2:/dst2")])
        );
        assert!(
            args.windows(2)
                .any(|w| w == [OsStr::new("--device"), OsStr::new("/dev/fuse")])
        );
        assert!(
            args.windows(2)
                .any(|w| w == [OsStr::new("--device"), OsStr::new("nvidia.com/gpu=all")])
        );
        assert!(
            args.windows(2)
                .any(|w| w == [OsStr::new("--env"), OsStr::new("TEST_1=EDF!")])
        );
        assert!(
            args.windows(2)
                .any(|w| w == [OsStr::new("--env"), OsStr::new("TEST_2=foobar")])
        );
        assert!(args.windows(2).any(|w| w
            == [
                OsStr::new("--annotation"),
                OsStr::new("com.hooks.test1.enabled=true")
            ]));
        assert!(args.windows(2).any(|w| w
            == [
                OsStr::new("--annotation"),
                OsStr::new("com.hooks.test2.enabled=false")
            ]));

        // Image and container command must be positionally at the end of args
        let (_, args_tail) = args.split_at(args.len() - 2);
        assert_eq!(args_tail[0], OsStr::new("ubuntu:24.04"));
        assert_eq!(args_tail[1], OsStr::new("bash"));
    }

    #[test]
    fn test_parallax_command() {
        let p_ctx = PodmanCtx {
            podman_path: PathBuf::from("/usr/bin/podman"),
            module: Some(String::from("hpc")),
            graphroot: Some(PathBuf::from("/dev/shm/sarus-test/graphroot")),
            runroot: Some(PathBuf::from("/dev/shm/sarus-test/runroot")),
            parallax_mount_program: Some(PathBuf::from(
                "/usr/local/sarus-test/parallax_mount_program",
            )),
            ro_store: Some(PathBuf::from("/scratch/user/parallax/store")),
        };

        let parallax_path = PathBuf::from("/usr/local/sarus-test/parallax");
        let image = String::from("ubuntu:24.04");

        let cmd = parallax_command(&parallax_path, &p_ctx, &image, "migrate");

        assert_eq!(cmd.get_program(), parallax_path);

        let args: Vec<&OsStr> = cmd.get_args().collect();
        assert_eq!(args.len(), 7);

        let args_expected: Vec<&OsStr> = vec![
            OsStr::new("--podmanRoot"),
            OsStr::new(p_ctx.graphroot.as_deref().unwrap()),
            OsStr::new("--roStoragePath"),
            OsStr::new(p_ctx.ro_store.as_deref().unwrap()),
            OsStr::new("--migrate"),
            OsStr::new("--image"),
            OsStr::new(&image),
        ];
        assert_eq!(args, args_expected);
    }
}
