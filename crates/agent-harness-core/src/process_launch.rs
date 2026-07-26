use std::io;
use std::process::Command;

use serde::{Deserialize, Serialize};

/// Configures a background or otherwise non-interactive child process.
///
/// On Windows, a console-subsystem executable launched by a process without an
/// attached console can otherwise allocate a visible console window. Runtime
/// services and captured-output helper commands must use this policy by
/// default. Explicitly interactive operator flows, such as browser or device
/// authentication, are intentionally excluded.
pub fn configure_noninteractive_command(command: &mut Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

        command.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    let _ = command;
}

/// The process-tree guarantee supplied by the active host adapter.
///
/// `TopLevelOnly` is useful for development and observation, but it is not a
/// production direct-owner boundary because grandchildren can survive an
/// abrupt master exit. Production direct ownership requires
/// `TreeKillOnOwnerExit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProcessTreeContainmentCapability {
    TopLevelOnly,
    TreeKillOnOwnerExit,
}

/// Arms a kill-on-close boundary for the lifetime of the current process.
///
/// This is used by a directly owned service wrapper that launches its own
/// descendants. The wrapper joins a nested Job and intentionally retains the
/// Job handle until process teardown. If the supervisor terminates only the
/// wrapper, Windows closes the retained handle and reaps its descendant tree
/// instead of leaving grandchildren alive inside the supervisor's broader
/// Job.
pub fn arm_process_lifetime_tree_containment() -> io::Result<ProcessTreeContainmentCapability> {
    let containment = MasterProcessTreeContainment::enter()?;
    let capability = containment.capability();
    std::mem::forget(containment);
    Ok(capability)
}

/// A process-wide containment boundary for the standalone supervisor master.
///
/// On Windows the master enters a non-inheritable kill-on-close Job before it
/// creates any child. Children and descendants therefore join the same Job
/// automatically. A clean shutdown must prove that no child remains and call
/// [`MasterProcessTreeContainment::disarm_after_children_exit`] before the
/// handle is closed. If the master dies abruptly, the kernel closes the handle
/// and reaps the remaining tree.
pub struct MasterProcessTreeContainment {
    capability: ProcessTreeContainmentCapability,
    #[cfg(windows)]
    job_handle: windows_sys::Win32::Foundation::HANDLE,
    #[cfg(windows)]
    armed: bool,
}

impl std::fmt::Debug for MasterProcessTreeContainment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MasterProcessTreeContainment")
            .field("capability", &self.capability)
            .field("armed", &{
                #[cfg(windows)]
                {
                    self.armed
                }
                #[cfg(not(windows))]
                {
                    false
                }
            })
            .finish_non_exhaustive()
    }
}

impl MasterProcessTreeContainment {
    /// Enters the strongest process-tree boundary implemented by this build.
    ///
    /// The current S1 implementation is production-capable on Windows. Other
    /// platforms deliberately report `TopLevelOnly`; the direct-owner command
    /// must reject that capability before spawning children until the S2 host
    /// adapters provide equivalent lifecycle containment.
    pub fn enter() -> io::Result<Self> {
        #[cfg(windows)]
        {
            use std::mem::{size_of, zeroed};
            use windows_sys::Win32::Foundation::CloseHandle;
            use windows_sys::Win32::System::JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
                SetInformationJobObject,
            };
            use windows_sys::Win32::System::Threading::GetCurrentProcess;

            unsafe {
                let job_handle = CreateJobObjectW(std::ptr::null(), std::ptr::null());
                if job_handle == 0 {
                    return Err(io::Error::last_os_error());
                }
                let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = zeroed();
                limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                if SetInformationJobObject(
                    job_handle,
                    JobObjectExtendedLimitInformation,
                    std::ptr::addr_of!(limits).cast(),
                    u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                        .expect("job information size fits u32"),
                ) == 0
                {
                    let error = io::Error::last_os_error();
                    let _ = CloseHandle(job_handle);
                    return Err(error);
                }
                if AssignProcessToJobObject(job_handle, GetCurrentProcess()) == 0 {
                    let error = io::Error::last_os_error();
                    let _ = CloseHandle(job_handle);
                    return Err(error);
                }
                Ok(Self {
                    capability: ProcessTreeContainmentCapability::TreeKillOnOwnerExit,
                    job_handle,
                    armed: true,
                })
            }
        }

        #[cfg(not(windows))]
        {
            Ok(Self {
                capability: ProcessTreeContainmentCapability::TopLevelOnly,
            })
        }
    }

    pub fn capability(&self) -> ProcessTreeContainmentCapability {
        self.capability
    }

    /// Returns the number of active processes in the Windows Job, including
    /// the master itself. Other platforms cannot make this claim yet.
    pub fn active_processes(&self) -> io::Result<Option<u32>> {
        #[cfg(windows)]
        {
            use std::mem::{size_of, zeroed};
            use windows_sys::Win32::System::JobObjects::{
                JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
                QueryInformationJobObject,
            };

            unsafe {
                let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = zeroed();
                if QueryInformationJobObject(
                    self.job_handle,
                    JobObjectBasicAccountingInformation,
                    std::ptr::addr_of_mut!(accounting).cast(),
                    u32::try_from(size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>())
                        .expect("job accounting information size fits u32"),
                    std::ptr::null_mut(),
                ) == 0
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(Some(accounting.ActiveProcesses))
            }
        }

        #[cfg(not(windows))]
        {
            Ok(None)
        }
    }

    /// Disarms kill-on-close only after the master has reaped every child.
    ///
    /// The Windows accounting count must be exactly one (the current master).
    /// Refusing to disarm when another process remains preserves the
    /// fail-closed tree guarantee.
    pub fn disarm_after_children_exit(&mut self) -> io::Result<()> {
        #[cfg(windows)]
        {
            use std::mem::{size_of, zeroed};
            use windows_sys::Win32::System::JobObjects::{
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
                SetInformationJobObject,
            };

            let active = self.active_processes()?.unwrap_or_default();
            if active != 1 {
                return Err(io::Error::other(format!(
                    "refused to disarm master Job while {active} process(es) remain"
                )));
            }
            unsafe {
                let limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = zeroed();
                if SetInformationJobObject(
                    self.job_handle,
                    JobObjectExtendedLimitInformation,
                    std::ptr::addr_of!(limits).cast(),
                    u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                        .expect("job information size fits u32"),
                ) == 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            self.armed = false;
        }
        Ok(())
    }
}

#[cfg(windows)]
impl Drop for MasterProcessTreeContainment {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::CloseHandle;

        unsafe {
            // When `armed` remains true, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE is
            // intentionally the final abrupt-master containment boundary.
            let _ = CloseHandle(self.job_handle);
        }
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::process::Stdio;
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    const PROCESS_LIFETIME_HELPER_ENV: &str = "AGENT_HARNESS_PROCESS_LIFETIME_TREE_HELPER";

    fn process_alive(pid: u32) -> bool {
        let output = Command::new("powershell.exe")
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ exit 0 }} else {{ exit 1 }}"
                ),
            ])
            .output()
            .expect("query process liveness");
        output.status.success()
    }

    fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(50));
        }
        predicate()
    }

    fn temp_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-{name}-{}-{unique}",
            std::process::id()
        ))
    }

    #[test]
    #[ignore = "invoked only as the subprocess side of the process-tree regression"]
    fn process_lifetime_tree_helper() {
        let Some(pid_file) = std::env::var_os(PROCESS_LIFETIME_HELPER_ENV).map(PathBuf::from)
        else {
            return;
        };
        assert_eq!(
            arm_process_lifetime_tree_containment().expect("arm helper containment"),
            ProcessTreeContainmentCapability::TreeKillOnOwnerExit
        );
        let mut child = Command::new("powershell.exe");
        child.args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "while ($true) { Start-Sleep -Milliseconds 100 }",
        ]);
        configure_noninteractive_command(&mut child);
        let child = child.spawn().expect("spawn helper descendant");
        fs::write(&pid_file, child.id().to_string()).expect("write descendant pid");
        loop {
            thread::sleep(Duration::from_secs(1));
        }
    }

    #[test]
    fn process_lifetime_tree_reaps_descendant_when_wrapper_is_terminated() {
        let root = temp_root("process-lifetime-tree");
        fs::create_dir_all(&root).expect("create test root");
        let pid_file = root.join("descendant.pid");
        let current_exe = std::env::current_exe().expect("current test executable");
        let mut wrapper = Command::new(current_exe);
        wrapper
            .args([
                "--exact",
                "process_launch::windows_tests::process_lifetime_tree_helper",
                "--ignored",
                "--nocapture",
            ])
            .env(PROCESS_LIFETIME_HELPER_ENV, &pid_file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        configure_noninteractive_command(&mut wrapper);
        let mut wrapper = wrapper.spawn().expect("spawn wrapper helper");
        assert!(
            wait_until(Duration::from_secs(10), || pid_file.is_file()),
            "wrapper did not publish descendant identity"
        );
        let descendant_pid: u32 = fs::read_to_string(&pid_file)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse descendant pid");
        assert!(
            process_alive(descendant_pid),
            "descendant was not alive before wrapper termination"
        );
        wrapper.kill().expect("terminate wrapper helper");
        wrapper.wait().expect("reap wrapper helper");
        assert!(
            wait_until(Duration::from_secs(10), || !process_alive(descendant_pid)),
            "descendant {descendant_pid} survived wrapper termination"
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn configured_noninteractive_console_process_has_no_console_window() {
        let script = concat!(
            "Add-Type -TypeDefinition '",
            "using System; ",
            "using System.Runtime.InteropServices; ",
            "public static class AgentHarnessConsoleProbe { ",
            "[DllImport(\"kernel32.dll\")] ",
            "public static extern IntPtr GetConsoleWindow(); ",
            "}' ; ",
            "[Console]::Out.Write(",
            "[AgentHarnessConsoleProbe]::GetConsoleWindow().ToInt64())"
        );
        let mut command = Command::new("powershell.exe");
        command.args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ]);
        configure_noninteractive_command(&mut command);

        let output = command
            .output()
            .expect("spawn configured PowerShell console probe");
        assert!(
            output.status.success(),
            "console probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            "0",
            "a configured non-interactive child must not attach or allocate a console window"
        );
    }
}
