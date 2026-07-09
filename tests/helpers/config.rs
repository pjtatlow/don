use std::fmt::Write;
use std::path::{Path, PathBuf};

/// Builder for programmatically generating `don.toml` content for tests.
///
/// Services and tasks are built with sub-builders that allow chaining
/// `depends_on` and other fields before finalizing with `.done()`.
pub struct ConfigBuilder {
    toml: String,
    has_service_groups_table: bool,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self {
            toml: String::new(),
            has_service_groups_table: false,
        }
    }

    /// Add a custom service with a run command. Call `.done()` to finalize.
    pub fn add_custom_service(self, name: &str, cmd: &str, args: &[&str]) -> ServiceBuilder {
        let mut lines = vec![format!("run.cmd = \"{cmd}\"")];
        if !args.is_empty() {
            let args_str: Vec<String> = args.iter().map(|a| format!("\"{a}\"")).collect();
            lines.push(format!("run.args = [{}]", args_str.join(", ")));
        }
        ServiceBuilder {
            builder: self,
            name: name.to_string(),
            lines,
        }
    }

    /// Add a docker service. Call `.done()` to finalize.
    pub fn add_docker_service(self, name: &str, image: &str) -> ServiceBuilder {
        ServiceBuilder {
            builder: self,
            name: name.to_string(),
            lines: vec![format!("docker.image = \"{image}\"")],
        }
    }

    /// Add a go service. Call `.done()` to finalize.
    pub fn add_go_service(self, name: &str, package: &str) -> ServiceBuilder {
        ServiceBuilder {
            builder: self,
            name: name.to_string(),
            lines: vec![format!("go.package = \"{package}\"")],
        }
    }

    /// Add a rust service. Call `.done()` to finalize.
    pub fn add_rust_service(self, name: &str, binary: &str) -> ServiceBuilder {
        ServiceBuilder {
            builder: self,
            name: name.to_string(),
            lines: vec![format!("rust.binary = \"{binary}\"")],
        }
    }

    /// Add a task. Call `.done()` to finalize.
    pub fn add_task(self, name: &str, cmd: &str, args: &[&str]) -> TaskBuilder {
        let mut lines = vec![format!("cmd = \"{cmd}\"")];
        if !args.is_empty() {
            let args_str: Vec<String> = args.iter().map(|a| format!("\"{a}\"")).collect();
            lines.push(format!("args = [{}]", args_str.join(", ")));
        }
        TaskBuilder {
            builder: self,
            name: name.to_string(),
            lines,
        }
    }

    /// Add a profile.
    pub fn add_profile(mut self, name: &str, services: &[&str], tasks: &[&str]) -> Self {
        writeln!(self.toml, "[profiles.{name}]").unwrap();
        if !services.is_empty() {
            let svc_str: Vec<String> = services.iter().map(|s| format!("\"{s}\"")).collect();
            writeln!(self.toml, "services = [{}]", svc_str.join(", ")).unwrap();
        }
        if !tasks.is_empty() {
            let task_str: Vec<String> = tasks.iter().map(|t| format!("\"{t}\"")).collect();
            writeln!(self.toml, "tasks = [{}]", task_str.join(", ")).unwrap();
        }
        writeln!(self.toml).unwrap();
        self
    }

    /// Add a named service group.
    pub fn add_service_group(mut self, name: &str, services: &[&str]) -> Self {
        let svc_str: Vec<String> = services.iter().map(|s| format!("\"{s}\"")).collect();
        if !self.has_service_groups_table {
            writeln!(self.toml, "[service_groups]").unwrap();
            self.has_service_groups_table = true;
        }
        writeln!(self.toml, "{name} = [{}]", svc_str.join(", ")).unwrap();
        writeln!(self.toml).unwrap();
        self
    }

    /// Set global watch ignore patterns.
    pub fn watch_ignore(mut self, patterns: &[&str]) -> Self {
        let p_str: Vec<String> = patterns.iter().map(|p| format!("\"{p}\"")).collect();
        writeln!(self.toml, "watch_ignore = [{}]", p_str.join(", ")).unwrap();
        writeln!(self.toml).unwrap();
        self
    }

    /// Append raw TOML content.
    pub fn raw(mut self, toml: &str) -> Self {
        writeln!(self.toml, "{toml}").unwrap();
        self
    }

    /// Get the generated TOML as a string.
    pub fn build(&self) -> String {
        self.toml.clone()
    }

    /// Write the generated TOML to `don.toml` in the given directory.
    /// Returns the path to the written file.
    pub fn write_to(&self, dir: &Path) -> PathBuf {
        let path = dir.join("don.toml");
        std::fs::write(&path, &self.toml).unwrap();
        path
    }
}

/// Builder for a service entry. Call `.done()` to finalize and return to `ConfigBuilder`.
pub struct ServiceBuilder {
    builder: ConfigBuilder,
    name: String,
    lines: Vec<String>,
}

impl ServiceBuilder {
    /// Add depends_on to this service.
    pub fn depends_on(mut self, deps: &[&str]) -> Self {
        let deps_str: Vec<String> = deps.iter().map(|d| format!("\"{d}\"")).collect();
        self.lines
            .push(format!("depends_on = [{}]", deps_str.join(", ")));
        self
    }

    /// Set the working directory.
    pub fn dir(mut self, dir: &str) -> Self {
        self.lines.push(format!("dir = \"{dir}\""));
        self
    }

    /// Set the log mode.
    pub fn log(mut self, log: &str) -> Self {
        self.lines.push(format!("log = \"{log}\""));
        self
    }

    /// Add an environment variable.
    pub fn env(mut self, key: &str, value: &str) -> Self {
        self.lines.push(format!("env.{key} = \"{value}\""));
        self
    }

    /// Add a TCP ready check.
    pub fn ready_tcp(mut self, addr: &str) -> Self {
        self.lines.push(format!("ready.tcp = \"{addr}\""));
        self
    }

    /// Add a TCP ready check with custom interval and retries.
    pub fn ready_tcp_with(mut self, addr: &str, interval: &str, retries: u32) -> Self {
        self.lines.push(format!("ready.tcp = \"{addr}\""));
        self.lines.push(format!("ready.interval = \"{interval}\""));
        self.lines.push(format!("ready.retries = {retries}"));
        self
    }

    /// Add an HTTP ready check.
    pub fn ready_http(mut self, url: &str) -> Self {
        self.lines.push(format!("ready.http = \"{url}\""));
        self
    }

    /// Add an HTTP ready check with custom interval and retries.
    pub fn ready_http_with(mut self, url: &str, interval: &str, retries: u32) -> Self {
        self.lines.push(format!("ready.http = \"{url}\""));
        self.lines.push(format!("ready.interval = \"{interval}\""));
        self.lines.push(format!("ready.retries = {retries}"));
        self
    }

    /// Add an exec ready check.
    pub fn ready_exec(mut self, cmd: &str, args: &[&str]) -> Self {
        self.lines.push(format!("ready.exec.cmd = \"{cmd}\""));
        if !args.is_empty() {
            let args_str: Vec<String> = args.iter().map(|a| format!("\"{a}\"")).collect();
            self.lines
                .push(format!("ready.exec.args = [{}]", args_str.join(", ")));
        }
        self
    }

    /// Add an exec ready check with custom interval and retries.
    pub fn ready_exec_with(
        mut self,
        cmd: &str,
        args: &[&str],
        interval: &str,
        retries: u32,
    ) -> Self {
        self.lines.push(format!("ready.exec.cmd = \"{cmd}\""));
        if !args.is_empty() {
            let args_str: Vec<String> = args.iter().map(|a| format!("\"{a}\"")).collect();
            self.lines
                .push(format!("ready.exec.args = [{}]", args_str.join(", ")));
        }
        self.lines.push(format!("ready.interval = \"{interval}\""));
        self.lines.push(format!("ready.retries = {retries}"));
        self
    }

    /// Set watch patterns.
    pub fn watch(mut self, patterns: &[&str]) -> Self {
        let p_str: Vec<String> = patterns.iter().map(|p| format!("\"{p}\"")).collect();
        self.lines.push(format!("watch = [{}]", p_str.join(", ")));
        self
    }

    /// Set ignore patterns.
    pub fn ignore(mut self, patterns: &[&str]) -> Self {
        let p_str: Vec<String> = patterns.iter().map(|p| format!("\"{p}\"")).collect();
        self.lines.push(format!("ignore = [{}]", p_str.join(", ")));
        self
    }

    /// Add listenfd-mode proxy entries (shorthand form). Each address
    /// becomes `proxy = { listen = "…", listenfd = true }` — don binds
    /// and hands the fd to the child via `LISTEN_FDS`.
    pub fn listen(self, addrs: &[&str]) -> Self {
        self.proxy_listenfd(addrs)
    }

    /// Add listenfd-mode proxy entries. Equivalent to
    /// `proxy = [{ listen = "...", listenfd = true }, ...]`.
    pub fn proxy_listenfd(mut self, addrs: &[&str]) -> Self {
        let entries: Vec<String> = addrs
            .iter()
            .map(|a| format!("{{ listen = \"{a}\", listenfd = true }}"))
            .collect();
        self.lines.push(format!("proxy = [{}]", entries.join(", ")));
        self
    }

    /// Add an env-mode proxy entry. Don accepts on `addr` and injects the
    /// ephemeral backend port into the service's environment as `env_name`.
    pub fn proxy_env(mut self, addr: &str, env_name: &str) -> Self {
        self.lines.push(format!(
            "proxy = {{ listen = \"{addr}\", env = \"{env_name}\" }}"
        ));
        self
    }

    /// Set reload (whether don watches files and rebuilds/restarts this service).
    pub fn reload(mut self, value: bool) -> Self {
        self.lines.push(format!("reload = {value}"));
        self
    }

    /// Mark this service as lazy.
    pub fn lazy(mut self, value: bool) -> Self {
        self.lines.push(format!("lazy = {value}"));
        self
    }

    /// Set debounce duration.
    pub fn debounce(mut self, duration: &str) -> Self {
        self.lines.push(format!("debounce = \"{duration}\""));
        self
    }

    /// Set build command.
    pub fn build_cmd(mut self, cmd: &str, args: &[&str]) -> Self {
        self.lines.push(format!("build.cmd = \"{cmd}\""));
        if !args.is_empty() {
            let args_str: Vec<String> = args.iter().map(|a| format!("\"{a}\"")).collect();
            self.lines
                .push(format!("build.args = [{}]", args_str.join(", ")));
        }
        self
    }

    /// Set shutdown config.
    pub fn shutdown(mut self, signal: &str, timeout: &str) -> Self {
        self.lines.push(format!("shutdown.signal = \"{signal}\""));
        self.lines.push(format!("shutdown.timeout = \"{timeout}\""));
        self
    }

    /// Enable or disable graceful shutdown for this service.
    pub fn graceful_shutdown(mut self, value: bool) -> Self {
        self.lines.push(format!("shutdown.graceful = {value}"));
        self
    }

    /// Finalize this service and return to the config builder.
    pub fn done(mut self) -> ConfigBuilder {
        writeln!(self.builder.toml, "[services.{}]", self.name).unwrap();
        for line in &self.lines {
            writeln!(self.builder.toml, "{line}").unwrap();
        }
        writeln!(self.builder.toml).unwrap();
        self.builder
    }

    // Convenience: allow writing directly without going back to ConfigBuilder
    /// Write the generated TOML to `don.toml` in the given directory.
    pub fn write_to(self, dir: &Path) -> PathBuf {
        self.done().write_to(dir)
    }
}

/// Builder for a task entry. Call `.done()` to finalize and return to `ConfigBuilder`.
pub struct TaskBuilder {
    builder: ConfigBuilder,
    name: String,
    lines: Vec<String>,
}

impl TaskBuilder {
    /// Add depends_on to this task.
    pub fn depends_on(mut self, deps: &[&str]) -> Self {
        let deps_str: Vec<String> = deps.iter().map(|d| format!("\"{d}\"")).collect();
        self.lines
            .push(format!("depends_on = [{}]", deps_str.join(", ")));
        self
    }

    /// Set the working directory.
    pub fn dir(mut self, dir: &str) -> Self {
        self.lines.push(format!("dir = \"{dir}\""));
        self
    }

    /// Set the log mode.
    pub fn log(mut self, log: &str) -> Self {
        self.lines.push(format!("log = \"{log}\""));
        self
    }

    /// Add an environment variable.
    pub fn env(mut self, key: &str, value: &str) -> Self {
        self.lines.push(format!("env.{key} = \"{value}\""));
        self
    }

    /// Set watch patterns.
    pub fn watch(mut self, patterns: &[&str]) -> Self {
        let p_str: Vec<String> = patterns.iter().map(|p| format!("\"{p}\"")).collect();
        self.lines.push(format!("watch = [{}]", p_str.join(", ")));
        self
    }

    /// Set ignore patterns.
    pub fn ignore(mut self, patterns: &[&str]) -> Self {
        let p_str: Vec<String> = patterns.iter().map(|p| format!("\"{p}\"")).collect();
        self.lines.push(format!("ignore = [{}]", p_str.join(", ")));
        self
    }

    /// Set timeout.
    pub fn timeout(mut self, timeout: &str) -> Self {
        self.lines.push(format!("timeout = \"{timeout}\""));
        self
    }

    /// Set auto_run.
    pub fn auto_run(mut self, value: bool) -> Self {
        self.lines.push(format!("auto_run = {value}"));
        self
    }

    /// Set auto_run to a named policy such as `"once"`.
    pub fn auto_run_mode(mut self, value: &str) -> Self {
        self.lines.push(format!("auto_run = \"{value}\""));
        self
    }

    /// Set the terminal mode (e.g. `"foreground"`).
    pub fn terminal_mode(mut self, mode: &str) -> Self {
        self.lines
            .push(format!("terminal = {{ mode = \"{mode}\", screen = \"main\" }}"));
        self
    }

    /// Finalize this task and return to the config builder.
    pub fn done(mut self) -> ConfigBuilder {
        writeln!(self.builder.toml, "[tasks.{}]", self.name).unwrap();
        for line in &self.lines {
            writeln!(self.builder.toml, "{line}").unwrap();
        }
        writeln!(self.builder.toml).unwrap();
        self.builder
    }

    /// Write the generated TOML to `don.toml` in the given directory.
    pub fn write_to(self, dir: &Path) -> PathBuf {
        self.done().write_to(dir)
    }
}
