use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::loop_health::process_alive_for_pid;
use crate::{
    append_jsonl_value, append_jsonl_value_once_by_event_key, current_log_time_ms,
    write_json_atomic,
};

pub const BACKEND_AUTH_STATE_SCHEMA: &str = "agent-harness.backend-auth-state.v1";
pub const BACKEND_AUTH_CONTINUATION_SCHEMA: &str = "agent-harness.backend-auth-continuation.v1";
pub const CODEX_BACKEND_AUTH_SCHEMA: &str = "agent-harness.codex-backend-auth.v1";
const BACKEND_AUTH_STATE_DIR: &str = "agent-harness";
const BACKEND_AUTH_STATE_FILE: &str = "backend-auth-state.json";
const BACKEND_AUTH_RECEIPTS_FILE: &str = "backend-auth-receipts.jsonl";
const BACKEND_AUTH_LEASE_FILE: &str = "backend-auth-operation.lock";
const BACKEND_AUTH_CANCEL_FILE: &str = "backend-auth-cancel.json";
const CODEX_BACKEND_AUTH_RECEIPTS_FILE: &str = "codex-backend-auth-receipts.jsonl";
const BACKEND_AUTH_UNKNOWN_OWNER_LEASE_TTL_MS: i64 = 15 * 60 * 1000;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendAuthLifecycleState {
    Unknown,
    Probing,
    Unauthenticated,
    LoginPending,
    Ready,
    RefreshRequired,
    Failed,
    Cancelled,
    Expired,
    LogoutPending,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendAuthStateV1 {
    pub schema: String,
    pub provider: String,
    pub lifecycle_state: BackendAuthLifecycleState,
    pub readiness_generation: u64,
    pub observed_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_openai_auth: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BackendAccountProbeOptions {
    pub harness_home: PathBuf,
    pub provider: String,
    pub codex_executable: PathBuf,
    pub executable_provenance_receipt: Option<PathBuf>,
    pub timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendAuthCliAction {
    BrowserLogin,
    DeviceCodeLogin,
    ApiKeyStdinLogin,
    Logout,
}

#[derive(Debug)]
pub struct BackendAuthCliOperationOptions {
    pub harness_home: PathBuf,
    pub provider: String,
    pub codex_executable: PathBuf,
    pub executable_provenance_receipt: Option<PathBuf>,
    pub action: BackendAuthCliAction,
    pub api_key_stdin: Option<Vec<u8>>,
    pub probe_timeout: Duration,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexBackendAuthReceiptV1 {
    pub schema: String,
    pub provider: String,
    pub provider_home_digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executable_provenance_receipt_ref: Option<String>,
    pub state: BackendAuthLifecycleState,
    pub transition: String,
    pub selected_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_correlation_digest: Option<String>,
    pub started_at_ms: i64,
    pub completed_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redacted_error_code: Option<String>,
    pub account_probe_result: String,
    pub capability_probe_result: String,
    pub readiness_generation: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendReadyDecision {
    pub ready: bool,
    pub readiness_generation: u64,
    pub lifecycle_state: BackendAuthLifecycleState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defer_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendAuthTransition {
    pub operation_id: String,
    pub next_state: BackendAuthLifecycleState,
    #[serde(default)]
    pub requires_openai_auth: Option<bool>,
    #[serde(default)]
    pub failure_code: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendAuthContinuationStatus {
    Waiting,
    Resumed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendAuthContinuationIntentV1 {
    pub schema: String,
    pub queue_id: String,
    pub provider: String,
    pub required_newer_than_generation: u64,
    pub status: BackendAuthContinuationStatus,
    pub defer_count: u64,
    pub resume_count: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendAuthContinuationDecision {
    pub intent: BackendAuthContinuationIntentV1,
    pub changed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendAuthCancelDecision {
    pub provider: String,
    pub operation_correlation_digest: String,
    pub requested_at_ms: i64,
    pub changed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendAuthDoctorReport {
    pub schema: String,
    pub provider: String,
    pub provider_home: PathBuf,
    pub provider_home_digest: String,
    pub lifecycle_state: BackendAuthLifecycleState,
    pub readiness_generation: u64,
    pub operation_lease_present: bool,
    pub cancel_request_present: bool,
    pub provider_home_permissions_restrictive: bool,
    pub latest_account_probe_result: Option<String>,
    pub latest_capability_probe_result: Option<String>,
    pub ready: bool,
    pub issues: Vec<String>,
}

#[derive(Debug)]
pub struct BackendAuthOperationLease {
    path: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct BackendAuthLeaseRecord {
    schema: String,
    provider: String,
    operation_id: String,
    owner_pid: i64,
    acquired_at_ms: i64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct BackendAuthCancelMarker {
    schema: String,
    provider: String,
    operation_correlation_digest: String,
    requested_at_ms: i64,
}

impl Drop for BackendAuthOperationLease {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn resolve_or_create_provider_codex_home(
    harness_home: &Path,
    provider: &str,
) -> io::Result<PathBuf> {
    let provider = normalized_provider(provider)?;
    let absolute_harness_home = if harness_home.is_absolute() {
        harness_home.to_path_buf()
    } else {
        std::env::current_dir()?.join(harness_home)
    };
    let candidate = if provider == "openai" {
        absolute_harness_home.join("codex-home")
    } else {
        absolute_harness_home
            .join("codex-home-providers")
            .join(provider)
    };
    let created = !candidate.exists();
    fs::create_dir_all(&candidate)?;
    if created {
        enforce_provider_home_permissions(&candidate)?;
    }
    candidate.canonicalize()
}

#[cfg(not(windows))]
fn enforce_provider_home_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(windows))]
fn provider_home_permissions_are_restrictive(path: &Path) -> io::Result<bool> {
    use std::os::unix::fs::PermissionsExt;
    Ok(fs::metadata(path)?.permissions().mode() & 0o077 == 0)
}

#[cfg(windows)]
fn with_current_user_sid<T>(
    operation: impl FnOnce(windows_sys::Win32::Foundation::PSID) -> io::Result<T>,
) -> io::Result<T> {
    use std::mem::size_of;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut token: HANDLE = 0;
    // SAFETY: the current process pseudo-handle is valid and token is an out parameter.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let result = (|| {
        let mut bytes = 0u32;
        // SAFETY: a null query buffer with length zero is the documented size probe.
        unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut bytes) };
        if bytes == 0 {
            return Err(io::Error::last_os_error());
        }
        let words = (bytes as usize).div_ceil(size_of::<usize>());
        let mut buffer = vec![0usize; words];
        // SAFETY: buffer is aligned, writable, and at least the queried byte length.
        if unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                bytes,
                &mut bytes,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful TokenUser output starts with TOKEN_USER and owns a valid SID.
        let token_user = unsafe { &*(buffer.as_ptr().cast::<TOKEN_USER>()) };
        operation(token_user.User.Sid)
    })();
    // SAFETY: token was returned by OpenProcessToken and is closed exactly once.
    unsafe { CloseHandle(token) };
    result
}

#[cfg(windows)]
fn enforce_provider_home_permissions(path: &Path) -> io::Result<()> {
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetNamedSecurityInfoW};
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, AddAccessAllowedAceEx, CONTAINER_INHERIT_ACE,
        DACL_SECURITY_INFORMATION, GetLengthSid, InitializeAcl, OBJECT_INHERIT_ACE,
        PROTECTED_DACL_SECURITY_INFORMATION,
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS;

    with_current_user_sid(|sid| {
        // SAFETY: sid comes from a live token buffer for the duration of this closure.
        let sid_bytes = unsafe { GetLengthSid(sid) } as usize;
        if sid_bytes == 0 {
            return Err(io::Error::last_os_error());
        }
        let acl_bytes = size_of::<ACL>()
            .saturating_add(size_of::<ACCESS_ALLOWED_ACE>() - size_of::<u32>())
            .saturating_add(sid_bytes);
        let mut acl_storage = vec![0usize; acl_bytes.div_ceil(size_of::<usize>())];
        let acl = acl_storage.as_mut_ptr().cast::<ACL>();
        // SAFETY: acl points to aligned writable storage of acl_bytes bytes.
        if unsafe { InitializeAcl(acl, acl_bytes as u32, ACL_REVISION) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: acl is initialized and sid is valid; inheritance is confined to this tree.
        if unsafe {
            AddAccessAllowedAceEx(
                acl,
                ACL_REVISION,
                CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE,
                FILE_ALL_ACCESS,
                sid,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut wide = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        // SAFETY: wide is null-terminated and acl remains live through the call.
        let result = unsafe {
            SetNamedSecurityInfoW(
                wide.as_mut_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                acl,
                std::ptr::null_mut(),
            )
        };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        Ok(())
    })
}

#[cfg(windows)]
fn provider_home_permissions_are_restrictive(path: &Path) -> io::Result<bool> {
    use std::mem::{size_of, zeroed};
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL, ACL_SIZE_INFORMATION, AclSizeInformation, CONTAINER_INHERIT_ACE,
        DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
        GetSecurityDescriptorControl, OBJECT_INHERIT_ACE, PSECURITY_DESCRIPTOR, SE_DACL_PROTECTED,
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS;

    with_current_user_sid(|sid| {
        let mut wide = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: all output pointers are valid and the path is null-terminated.
        let result = unsafe {
            GetNamedSecurityInfoW(
                wide.as_mut_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        let checked = (|| {
            if dacl.is_null() || descriptor.is_null() {
                return Ok(false);
            }
            // SAFETY: successful GetNamedSecurityInfoW returned a valid descriptor.
            let mut control = 0u16;
            let mut revision = 0u32;
            if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
                || control & SE_DACL_PROTECTED == 0
            {
                return Ok(false);
            }
            // SAFETY: dacl is valid for the lifetime of descriptor.
            let mut info: ACL_SIZE_INFORMATION = unsafe { zeroed() };
            if unsafe {
                GetAclInformation(
                    dacl,
                    (&mut info as *mut ACL_SIZE_INFORMATION).cast(),
                    size_of::<ACL_SIZE_INFORMATION>() as u32,
                    AclSizeInformation,
                )
            } == 0
                || info.AceCount != 1
            {
                return Ok(false);
            }
            let mut ace_ptr = std::ptr::null_mut();
            // SAFETY: the ACL reports one ACE and index zero is valid.
            if unsafe { GetAce(dacl, 0, &mut ace_ptr) } == 0 || ace_ptr.is_null() {
                return Ok(false);
            }
            // SAFETY: the only ACE was created as ACCESS_ALLOWED_ACE by enforcement.
            let ace = unsafe { &*(ace_ptr.cast::<ACCESS_ALLOWED_ACE>()) };
            let ace_sid = std::ptr::addr_of!(ace.SidStart)
                .cast_mut()
                .cast::<core::ffi::c_void>();
            let inheritance = (CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE) as u8;
            // AceType 0 is ACCESS_ALLOWED_ACE_TYPE.
            Ok(ace.Header.AceType == 0
                && ace.Header.AceFlags & inheritance == inheritance
                && ace.Mask & FILE_ALL_ACCESS == FILE_ALL_ACCESS
                // SAFETY: ace_sid points to the SID tail of a valid access-allowed ACE.
                && unsafe { EqualSid(ace_sid, sid) } != 0)
        })();
        // SAFETY: descriptor was allocated by GetNamedSecurityInfoW.
        unsafe { LocalFree(descriptor) };
        checked
    })
}

pub fn load_backend_auth_state(
    codex_home: &Path,
    provider: &str,
) -> io::Result<BackendAuthStateV1> {
    let state_file = backend_auth_state_file(codex_home);
    if !state_file.is_file() {
        return Ok(new_state(provider, BackendAuthLifecycleState::Unknown, 0));
    }
    let state: BackendAuthStateV1 = serde_json::from_slice(&fs::read(state_file)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if state.schema != BACKEND_AUTH_STATE_SCHEMA || state.provider != normalized_provider(provider)?
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backend auth state schema/provider mismatch",
        ));
    }
    Ok(state)
}

pub fn persist_backend_auth_state(codex_home: &Path, state: &BackendAuthStateV1) -> io::Result<()> {
    validate_secret_free_state(state)?;
    let state_dir = codex_home.join(BACKEND_AUTH_STATE_DIR);
    fs::create_dir_all(&state_dir)?;
    write_json_atomic(&state_dir.join(BACKEND_AUTH_STATE_FILE), state)?;
    append_jsonl_value(&state_dir.join(BACKEND_AUTH_RECEIPTS_FILE), state)
}

pub fn acquire_backend_auth_operation_lease(
    codex_home: &Path,
    provider: &str,
    operation_id: &str,
) -> io::Result<BackendAuthOperationLease> {
    if operation_id.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backend auth operation id must not be empty",
        ));
    }
    let state_dir = codex_home.join(BACKEND_AUTH_STATE_DIR);
    fs::create_dir_all(&state_dir)?;
    let path = state_dir.join(BACKEND_AUTH_LEASE_FILE);
    let lease = BackendAuthLeaseRecord {
        schema: BACKEND_AUTH_STATE_SCHEMA.to_string(),
        provider: normalized_provider(provider)?,
        operation_id: operation_id.to_string(),
        owner_pid: i64::from(std::process::id()),
        acquired_at_ms: current_log_time_ms()?,
    };
    let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let existing = fs::read(&path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<BackendAuthLeaseRecord>(&bytes).ok());
            let reclaimable = existing.as_ref().is_some_and(|record| {
                match process_alive_for_pid(record.owner_pid) {
                    Some(false) => true,
                    Some(true) => false,
                    None => current_log_time_ms().is_ok_and(|now| {
                        now.saturating_sub(record.acquired_at_ms)
                            > BACKEND_AUTH_UNKNOWN_OWNER_LEASE_TTL_MS
                    }),
                }
            });
            if reclaimable {
                fs::remove_file(&path)?;
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)?
            } else {
                return Err(error);
            }
        }
        Err(error) => return Err(error),
    };
    serde_json::to_writer(&mut file, &lease).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(BackendAuthOperationLease { path })
}

pub fn begin_backend_auth_operation(
    codex_home: &Path,
    provider: &str,
    operation_id: &str,
    next_state: BackendAuthLifecycleState,
) -> io::Result<BackendAuthStateV1> {
    if !matches!(
        next_state,
        BackendAuthLifecycleState::Probing
            | BackendAuthLifecycleState::LoginPending
            | BackendAuthLifecycleState::LogoutPending
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backend auth operation must begin in probing, login-pending, or logout-pending",
        ));
    }
    let current = load_backend_auth_state(codex_home, provider)?;
    validate_backend_auth_transition(current.lifecycle_state, next_state)?;
    let mut next = new_state(
        provider,
        next_state,
        current.readiness_generation.saturating_add(1),
    );
    let operation_id = nonempty_operation_id(operation_id)?;
    next.operation_id = Some(auth_operation_correlation_digest(&operation_id));
    persist_backend_auth_state(codex_home, &next)?;
    Ok(next)
}

pub fn complete_backend_auth_operation(
    codex_home: &Path,
    provider: &str,
    transition: BackendAuthTransition,
) -> io::Result<BackendAuthStateV1> {
    let current = load_backend_auth_state(codex_home, provider)?;
    let correlation_digest = auth_operation_correlation_digest(&transition.operation_id);
    if current.operation_id.as_deref() != Some(correlation_digest.as_str()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backend auth completion does not correlate to the active operation",
        ));
    }
    if matches!(
        transition.next_state,
        BackendAuthLifecycleState::Probing
            | BackendAuthLifecycleState::LoginPending
            | BackendAuthLifecycleState::LogoutPending
            | BackendAuthLifecycleState::Unknown
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backend auth completion must be terminal or require operator action",
        ));
    }
    validate_backend_auth_transition(current.lifecycle_state, transition.next_state)?;
    let mut next = new_state(
        provider,
        transition.next_state,
        current.readiness_generation,
    );
    next.requires_openai_auth = transition.requires_openai_auth;
    next.failure_code = transition.failure_code;
    persist_backend_auth_state(codex_home, &next)?;
    Ok(next)
}

pub fn expire_backend_auth_operation(
    codex_home: &Path,
    provider: &str,
    operation_id: &str,
) -> io::Result<BackendAuthStateV1> {
    complete_backend_auth_operation(
        codex_home,
        provider,
        BackendAuthTransition {
            operation_id: operation_id.to_string(),
            next_state: BackendAuthLifecycleState::Expired,
            requires_openai_auth: None,
            failure_code: Some("operator-auth-expired".to_string()),
        },
    )
}

pub fn probe_backend_account(
    options: BackendAccountProbeOptions,
) -> io::Result<BackendAuthStateV1> {
    let provider = normalized_provider(&options.provider)?;
    let codex_home = resolve_or_create_provider_codex_home(&options.harness_home, &provider)?;
    let lease_operation_id = format!("account-read-lease-{}", current_log_time_ms()?);
    let _lease = acquire_backend_auth_operation_lease(&codex_home, &provider, &lease_operation_id)?;
    probe_backend_account_under_lease(options)
}

fn probe_backend_account_under_lease(
    options: BackendAccountProbeOptions,
) -> io::Result<BackendAuthStateV1> {
    let provider = normalized_provider(&options.provider)?;
    let codex_home = resolve_or_create_provider_codex_home(&options.harness_home, &provider)?;
    let started_at_ms = current_log_time_ms()?;
    let operation_id = format!("account-read-probe-{started_at_ms}");
    let probing = begin_backend_auth_operation(
        &codex_home,
        &provider,
        &operation_id,
        BackendAuthLifecycleState::Probing,
    )?;
    let generation = probing.readiness_generation;

    let executable = options.codex_executable.canonicalize().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("backend auth Codex executable is not canonical: {error}"),
        )
    })?;
    let executable_provenance_receipt_ref = resolve_executable_provenance_receipt_ref(
        options.executable_provenance_receipt.as_deref(),
        &executable,
    )?;
    let mut child = Command::new(executable)
        .arg("app-server")
        .arg("--stdio")
        .env("CODEX_HOME", &codex_home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut stdin = child.stdin.take().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "Codex app-server stdin unavailable",
        )
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "Codex app-server stdout unavailable",
        )
    })?;

    writeln!(
        stdin,
        "{}",
        json!({"id":0,"method":"initialize","params":{"clientInfo":{"name":"agent-harness-auth-probe","version":"1"},"capabilities":{}}})
    )?;
    writeln!(stdin, "{}", json!({"method":"initialized","params":{}}))?;
    writeln!(
        stdin,
        "{}",
        json!({"id":1,"method":"account/read","params":{"refreshToken":true}})
    )?;
    writeln!(
        stdin,
        "{}",
        json!({"id":2,"method":"model/list","params":{}})
    )?;
    stdin.flush()?;

    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) => {
                    if sender.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let deadline = Instant::now() + options.timeout;
    let mut account_response = None;
    let mut capability_response = None;
    let response = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for account/read",
            ));
        }
        match receiver.recv_timeout(remaining) {
            Ok(line) => {
                let value: Value = match serde_json::from_str(&line) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                match value.get("id").and_then(Value::as_u64) {
                    Some(1) => account_response = Some(value),
                    Some(2) => capability_response = Some(value),
                    _ => {}
                }
                if let Some(account) = account_response.as_ref() {
                    let account_state = classify_account_read(&provider, generation, account);
                    if account_state.lifecycle_state != BackendAuthLifecycleState::Ready
                        || capability_response.is_some()
                    {
                        break Ok((account_response.take().unwrap(), capability_response.take()));
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                break Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for account/read",
                ));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Codex app-server closed before account/read",
                ));
            }
        }
    };

    let _ = terminate_backend_auth_child_process_tree(&mut child);
    let (state, account_probe_result, capability_probe_result, auth_mode, plan_type) =
        match response {
            Ok((value, capability)) => {
                let mut state = classify_account_read(&provider, generation, &value);
                let account_probe_result =
                    backend_auth_lifecycle_label(state.lifecycle_state).to_string();
                let capability_probe_result =
                    if state.lifecycle_state == BackendAuthLifecycleState::Ready {
                        if capability.as_ref().is_some_and(model_list_capability_ready) {
                            "ready".to_string()
                        } else {
                            state.lifecycle_state = BackendAuthLifecycleState::RefreshRequired;
                            state.failure_code = Some("model-capability-probe-failed".to_string());
                            "failed".to_string()
                        }
                    } else {
                        "skipped-not-ready".to_string()
                    };
                let auth_mode =
                    safe_optional_string(&value, &["/result/authMode", "/result/auth_mode"]);
                let plan_type = safe_optional_string(
                    &value,
                    &[
                        "/result/planType",
                        "/result/plan_type",
                        "/result/account/planType",
                    ],
                );
                (
                    state,
                    account_probe_result,
                    capability_probe_result,
                    auth_mode,
                    plan_type,
                )
            }
            Err(error) => {
                let mut state = new_state(&provider, BackendAuthLifecycleState::Failed, generation);
                state.failure_code = Some(
                    match error.kind() {
                        io::ErrorKind::TimedOut => "account-read-timeout",
                        io::ErrorKind::UnexpectedEof => "account-read-eof",
                        _ => "account-read-failed",
                    }
                    .to_string(),
                );
                (
                    state,
                    "failed".to_string(),
                    "not-run".to_string(),
                    None,
                    None,
                )
            }
        };
    let completed = complete_backend_auth_operation(
        &codex_home,
        &provider,
        BackendAuthTransition {
            operation_id,
            next_state: state.lifecycle_state,
            requires_openai_auth: state.requires_openai_auth,
            failure_code: state.failure_code,
        },
    )?;
    if completed.lifecycle_state == BackendAuthLifecycleState::Ready {
        let _ =
            resume_all_backend_auth_defer_intents(&options.harness_home, &provider, &completed)?;
    }
    let receipt = CodexBackendAuthReceiptV1 {
        schema: CODEX_BACKEND_AUTH_SCHEMA.to_string(),
        provider: provider.clone(),
        provider_home_digest: path_identity_digest(&codex_home),
        executable_provenance_receipt_ref,
        state: completed.lifecycle_state,
        transition: "account-probe-completed".to_string(),
        selected_method: "account-read-refresh".to_string(),
        operation_correlation_digest: probing.operation_id,
        started_at_ms,
        completed_at_ms: current_log_time_ms()?,
        expires_at_ms: None,
        auth_mode,
        plan_type,
        redacted_error_code: completed.failure_code.clone(),
        account_probe_result,
        capability_probe_result,
        readiness_generation: completed.readiness_generation,
    };
    append_codex_backend_auth_receipt(&codex_home, &receipt)?;
    Ok(completed)
}

pub fn run_backend_auth_cli_operation(
    mut options: BackendAuthCliOperationOptions,
) -> io::Result<BackendAuthStateV1> {
    if options.action == BackendAuthCliAction::ApiKeyStdinLogin && options.api_key_stdin.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "API-key login requires key bytes from operator stdin",
        ));
    }
    let provider = normalized_provider(&options.provider)?;
    let codex_home = resolve_or_create_provider_codex_home(&options.harness_home, &provider)?;
    let executable = options.codex_executable.canonicalize()?;
    let executable_provenance_receipt_ref = resolve_executable_provenance_receipt_ref(
        options.executable_provenance_receipt.as_deref(),
        &executable,
    )?;
    let started_at_ms = current_log_time_ms()?;
    let operation_id = format!("operator-auth-{started_at_ms}");
    let correlation_digest = auth_operation_correlation_digest(&operation_id);
    let selected_method = backend_auth_cli_method_label(options.action);
    let _lease = acquire_backend_auth_operation_lease(&codex_home, &provider, &operation_id)?;
    let cancel_file = backend_auth_cancel_file(&codex_home);
    match fs::remove_file(&cancel_file) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let pending_state = match options.action {
        BackendAuthCliAction::Logout => BackendAuthLifecycleState::LogoutPending,
        _ => BackendAuthLifecycleState::LoginPending,
    };
    let pending =
        begin_backend_auth_operation(&codex_home, &provider, &operation_id, pending_state)?;
    append_codex_backend_auth_receipt(
        &codex_home,
        &CodexBackendAuthReceiptV1 {
            schema: CODEX_BACKEND_AUTH_SCHEMA.to_string(),
            provider: provider.clone(),
            provider_home_digest: path_identity_digest(&codex_home),
            executable_provenance_receipt_ref: executable_provenance_receipt_ref.clone(),
            state: pending.lifecycle_state,
            transition: "operator-operation-started".to_string(),
            selected_method: selected_method.to_string(),
            operation_correlation_digest: pending.operation_id,
            started_at_ms,
            completed_at_ms: started_at_ms,
            expires_at_ms: None,
            auth_mode: None,
            plan_type: None,
            redacted_error_code: None,
            account_probe_result: "not-run".to_string(),
            capability_probe_result: "not-run".to_string(),
            readiness_generation: pending.readiness_generation,
        },
    )?;

    let mut command = Command::new(executable);
    command.env("CODEX_HOME", &codex_home);
    match options.action {
        BackendAuthCliAction::BrowserLogin => {
            command.arg("login").stdin(Stdio::inherit());
        }
        BackendAuthCliAction::DeviceCodeLogin => {
            command
                .arg("login")
                .arg("--device-auth")
                .stdin(Stdio::inherit());
        }
        BackendAuthCliAction::ApiKeyStdinLogin => {
            command
                .arg("login")
                .arg("--with-api-key")
                .stdin(Stdio::piped());
        }
        BackendAuthCliAction::Logout => {
            command.arg("logout").stdin(Stdio::null());
        }
    }
    command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            return complete_backend_auth_cli_operation_with_receipt(
                &codex_home,
                &provider,
                &operation_id,
                BackendAuthLifecycleState::Failed,
                Some("operator-auth-process-spawn-failed".to_string()),
                selected_method,
                started_at_ms,
                &correlation_digest,
                executable_provenance_receipt_ref,
            );
        }
    };
    if options.action == BackendAuthCliAction::ApiKeyStdinLogin {
        let mut api_key = options.api_key_stdin.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "API-key login requires key bytes from operator stdin",
            )
        })?;
        let write_result = child
            .stdin
            .take()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "Codex login stdin unavailable")
            })
            .and_then(|mut stdin| {
                stdin.write_all(&api_key)?;
                if !api_key.ends_with(b"\n") {
                    stdin.write_all(b"\n")?;
                }
                stdin.flush()
            });
        api_key.fill(0);
        if write_result.is_err() {
            let _ = terminate_backend_auth_child_process_tree(&mut child);
            return complete_backend_auth_cli_operation_with_receipt(
                &codex_home,
                &provider,
                &operation_id,
                BackendAuthLifecycleState::Failed,
                Some("operator-api-key-stdin-failed".to_string()),
                selected_method,
                started_at_ms,
                &correlation_digest,
                executable_provenance_receipt_ref,
            );
        }
    }
    let (status, cancel_requested) =
        wait_for_backend_auth_child(&mut child, &cancel_file, &provider, &correlation_digest)?;
    let _ = fs::remove_file(&cancel_file);
    if cancel_requested {
        return complete_backend_auth_cli_operation_with_receipt(
            &codex_home,
            &provider,
            &operation_id,
            BackendAuthLifecycleState::Cancelled,
            Some("operator-auth-cancelled".to_string()),
            selected_method,
            started_at_ms,
            &correlation_digest,
            executable_provenance_receipt_ref,
        );
    }
    if !status.success() {
        return complete_backend_auth_cli_operation_with_receipt(
            &codex_home,
            &provider,
            &operation_id,
            if pending_state == BackendAuthLifecycleState::LoginPending {
                BackendAuthLifecycleState::Cancelled
            } else {
                BackendAuthLifecycleState::Failed
            },
            Some(
                if pending_state == BackendAuthLifecycleState::LoginPending {
                    "operator-auth-cancelled"
                } else {
                    "operator-logout-failed"
                }
                .to_string(),
            ),
            selected_method,
            started_at_ms,
            &correlation_digest,
            executable_provenance_receipt_ref,
        );
    }

    let completed = probe_backend_account_under_lease(BackendAccountProbeOptions {
        harness_home: options.harness_home,
        provider,
        codex_executable: options.codex_executable,
        executable_provenance_receipt: options.executable_provenance_receipt,
        timeout: options.probe_timeout,
    })?;
    append_codex_backend_auth_receipt(
        &codex_home,
        &CodexBackendAuthReceiptV1 {
            schema: CODEX_BACKEND_AUTH_SCHEMA.to_string(),
            provider: completed.provider.clone(),
            provider_home_digest: path_identity_digest(&codex_home),
            executable_provenance_receipt_ref,
            state: completed.lifecycle_state,
            transition: "operator-operation-reconciled".to_string(),
            selected_method: selected_method.to_string(),
            operation_correlation_digest: Some(correlation_digest),
            started_at_ms,
            completed_at_ms: current_log_time_ms()?,
            expires_at_ms: None,
            auth_mode: None,
            plan_type: None,
            redacted_error_code: completed.failure_code.clone(),
            account_probe_result: backend_auth_lifecycle_label(completed.lifecycle_state)
                .to_string(),
            capability_probe_result: if completed.lifecycle_state
                == BackendAuthLifecycleState::Ready
            {
                "ready"
            } else {
                "not-ready"
            }
            .to_string(),
            readiness_generation: completed.readiness_generation,
        },
    )?;
    Ok(completed)
}

pub fn request_backend_auth_cancel(
    harness_home: &Path,
    provider: &str,
) -> io::Result<BackendAuthCancelDecision> {
    let provider = normalized_provider(provider)?;
    let codex_home = resolve_or_create_provider_codex_home(harness_home, &provider)?;
    let state = load_backend_auth_state(&codex_home, &provider)?;
    if !matches!(
        state.lifecycle_state,
        BackendAuthLifecycleState::LoginPending | BackendAuthLifecycleState::LogoutPending
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backend auth cancel requires a login-pending or logout-pending operation",
        ));
    }
    let operation_correlation_digest = state.operation_id.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "pending backend auth state has no operation correlation digest",
        )
    })?;
    let file = backend_auth_cancel_file(&codex_home);
    if file.is_file() {
        let existing: BackendAuthCancelMarker = serde_json::from_slice(&fs::read(&file)?)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if existing.schema == BACKEND_AUTH_STATE_SCHEMA
            && existing.provider == provider
            && existing.operation_correlation_digest == operation_correlation_digest
        {
            return Ok(BackendAuthCancelDecision {
                provider,
                operation_correlation_digest,
                requested_at_ms: existing.requested_at_ms,
                changed: false,
            });
        }
    }
    let marker = BackendAuthCancelMarker {
        schema: BACKEND_AUTH_STATE_SCHEMA.to_string(),
        provider: provider.clone(),
        operation_correlation_digest: operation_correlation_digest.clone(),
        requested_at_ms: current_log_time_ms()?,
    };
    write_json_atomic(&file, &marker)?;
    Ok(BackendAuthCancelDecision {
        provider,
        operation_correlation_digest,
        requested_at_ms: marker.requested_at_ms,
        changed: true,
    })
}

pub fn doctor_backend_auth(
    harness_home: &Path,
    provider: &str,
) -> io::Result<BackendAuthDoctorReport> {
    let provider = normalized_provider(provider)?;
    let codex_home = resolve_or_create_provider_codex_home(harness_home, &provider)?;
    let state = load_backend_auth_state(&codex_home, &provider)?;
    validate_secret_free_state(&state)?;
    let state_dir = codex_home.join(BACKEND_AUTH_STATE_DIR);
    let operation_lease_present = state_dir.join(BACKEND_AUTH_LEASE_FILE).is_file();
    let cancel_request_present = state_dir.join(BACKEND_AUTH_CANCEL_FILE).is_file();
    let provider_home_permissions_restrictive =
        provider_home_permissions_are_restrictive(&codex_home)?;
    let latest_receipt = latest_codex_backend_auth_receipt(&codex_home)?;
    let latest_account_probe_result = latest_receipt
        .as_ref()
        .map(|receipt| receipt.account_probe_result.clone());
    let latest_capability_probe_result = latest_receipt
        .as_ref()
        .map(|receipt| receipt.capability_probe_result.clone());
    let capability_generation_ready = latest_receipt.as_ref().is_some_and(|receipt| {
        receipt.schema == CODEX_BACKEND_AUTH_SCHEMA
            && receipt.provider == provider
            && receipt.readiness_generation == state.readiness_generation
            && receipt.account_probe_result == "ready"
            && receipt.capability_probe_result == "ready"
    });
    let ready = state.lifecycle_state == BackendAuthLifecycleState::Ready
        && capability_generation_ready
        && !operation_lease_present
        && !cancel_request_present
        && provider_home_permissions_restrictive;
    let mut issues = Vec::new();
    if matches!(
        state.lifecycle_state,
        BackendAuthLifecycleState::LoginPending | BackendAuthLifecycleState::LogoutPending
    ) && !operation_lease_present
    {
        issues.push("pending auth state has no live provider operation lease".to_string());
    }
    if state.lifecycle_state == BackendAuthLifecycleState::Ready && !capability_generation_ready {
        issues.push(
            "ready state lacks same-generation account and model-capability evidence".to_string(),
        );
    }
    if cancel_request_present && !operation_lease_present {
        issues.push("orphaned backend auth cancel request requires reconciliation".to_string());
    }
    if !provider_home_permissions_restrictive {
        issues.push("provider home permissions are not current-operator-only".to_string());
    }
    Ok(BackendAuthDoctorReport {
        schema: "agent-harness.backend-auth-doctor.v1".to_string(),
        provider,
        provider_home: codex_home.clone(),
        provider_home_digest: path_identity_digest(&codex_home),
        lifecycle_state: state.lifecycle_state,
        readiness_generation: state.readiness_generation,
        operation_lease_present,
        cancel_request_present,
        provider_home_permissions_restrictive,
        latest_account_probe_result,
        latest_capability_probe_result,
        ready,
        issues,
    })
}

#[allow(clippy::too_many_arguments)]
fn complete_backend_auth_cli_operation_with_receipt(
    codex_home: &Path,
    provider: &str,
    operation_id: &str,
    next_state: BackendAuthLifecycleState,
    failure_code: Option<String>,
    selected_method: &str,
    started_at_ms: i64,
    operation_correlation_digest: &str,
    executable_provenance_receipt_ref: Option<String>,
) -> io::Result<BackendAuthStateV1> {
    let state = complete_backend_auth_operation(
        codex_home,
        provider,
        BackendAuthTransition {
            operation_id: operation_id.to_string(),
            next_state,
            requires_openai_auth: None,
            failure_code,
        },
    )?;
    append_codex_backend_auth_receipt(
        codex_home,
        &CodexBackendAuthReceiptV1 {
            schema: CODEX_BACKEND_AUTH_SCHEMA.to_string(),
            provider: provider.to_string(),
            provider_home_digest: path_identity_digest(codex_home),
            executable_provenance_receipt_ref,
            state: state.lifecycle_state,
            transition: "operator-operation-completed".to_string(),
            selected_method: selected_method.to_string(),
            operation_correlation_digest: Some(operation_correlation_digest.to_string()),
            started_at_ms,
            completed_at_ms: current_log_time_ms()?,
            expires_at_ms: None,
            auth_mode: None,
            plan_type: None,
            redacted_error_code: state.failure_code.clone(),
            account_probe_result: "not-run".to_string(),
            capability_probe_result: "not-run".to_string(),
            readiness_generation: state.readiness_generation,
        },
    )?;
    Ok(state)
}

fn backend_auth_cli_method_label(action: BackendAuthCliAction) -> &'static str {
    match action {
        BackendAuthCliAction::BrowserLogin => "chatgpt-browser",
        BackendAuthCliAction::DeviceCodeLogin => "chatgpt-device-code",
        BackendAuthCliAction::ApiKeyStdinLogin => "api-key-stdin",
        BackendAuthCliAction::Logout => "logout",
    }
}

pub fn require_backend_ready_for_turn(
    state: &BackendAuthStateV1,
    expected_generation: u64,
) -> BackendReadyDecision {
    let generation_matches = state.readiness_generation == expected_generation;
    let ready = generation_matches && state.lifecycle_state == BackendAuthLifecycleState::Ready;
    BackendReadyDecision {
        ready,
        readiness_generation: state.readiness_generation,
        lifecycle_state: state.lifecycle_state,
        defer_reason: (!ready).then(|| {
            if !generation_matches {
                "stale-auth-readiness-generation".to_string()
            } else {
                "needs-operator-auth".to_string()
            }
        }),
    }
}

pub fn backend_auth_runtime_gate_enabled(harness_home: &Path) -> io::Result<bool> {
    let config_file = [
        harness_home.join("harness-config.json"),
        harness_home.join("config").join("harness-config.json"),
    ]
    .into_iter()
    .find(|path| path.is_file());
    let Some(config_file) = config_file else {
        return Ok(false);
    };
    let value: Value = serde_json::from_slice(&fs::read(config_file)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(value
        .pointer("/backendAuth/runtimeGateEnabled")
        .and_then(Value::as_bool)
        .unwrap_or(false))
}

pub fn record_backend_auth_defer_intent(
    harness_home: &Path,
    queue_id: &str,
    provider: &str,
    observed_generation: u64,
) -> io::Result<BackendAuthContinuationDecision> {
    let file = backend_auth_continuation_file(harness_home, queue_id);
    if file.is_file() {
        let existing: BackendAuthContinuationIntentV1 =
            serde_json::from_slice(&fs::read(&file)?)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        validate_continuation_identity(&existing, queue_id, provider)?;
        if existing.status == BackendAuthContinuationStatus::Waiting
            && existing.required_newer_than_generation >= observed_generation
        {
            return Ok(BackendAuthContinuationDecision {
                intent: existing,
                changed: false,
            });
        }
    }
    let now = current_log_time_ms()?;
    let intent = BackendAuthContinuationIntentV1 {
        schema: BACKEND_AUTH_CONTINUATION_SCHEMA.to_string(),
        queue_id: queue_id.to_string(),
        provider: normalized_provider(provider)?,
        required_newer_than_generation: observed_generation,
        status: BackendAuthContinuationStatus::Waiting,
        defer_count: 1,
        resume_count: 0,
        created_at_ms: now,
        updated_at_ms: now,
    };
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)?;
    }
    write_json_atomic(&file, &intent)?;
    append_jsonl_value(
        &backend_auth_continuation_receipts_file(harness_home),
        &intent,
    )?;
    Ok(BackendAuthContinuationDecision {
        intent,
        changed: true,
    })
}

pub fn resume_backend_auth_defer_intent(
    harness_home: &Path,
    queue_id: &str,
    provider: &str,
    ready_state: &BackendAuthStateV1,
) -> io::Result<Option<BackendAuthContinuationDecision>> {
    let file = backend_auth_continuation_file(harness_home, queue_id);
    if !file.is_file() {
        return Ok(None);
    }
    let mut intent: BackendAuthContinuationIntentV1 = serde_json::from_slice(&fs::read(&file)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    validate_continuation_identity(&intent, queue_id, provider)?;
    if intent.status == BackendAuthContinuationStatus::Resumed {
        return Ok(Some(BackendAuthContinuationDecision {
            intent,
            changed: false,
        }));
    }
    if ready_state.lifecycle_state != BackendAuthLifecycleState::Ready
        || ready_state.provider != normalized_provider(provider)?
        || ready_state.readiness_generation <= intent.required_newer_than_generation
    {
        return Ok(Some(BackendAuthContinuationDecision {
            intent,
            changed: false,
        }));
    }
    intent.status = BackendAuthContinuationStatus::Resumed;
    intent.resume_count = 1;
    intent.updated_at_ms = current_log_time_ms()?;
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)?;
    }
    write_json_atomic(&file, &intent)?;
    append_jsonl_value(
        &backend_auth_continuation_receipts_file(harness_home),
        &intent,
    )?;
    Ok(Some(BackendAuthContinuationDecision {
        intent,
        changed: true,
    }))
}

pub fn resume_all_backend_auth_defer_intents(
    harness_home: &Path,
    provider: &str,
    ready_state: &BackendAuthStateV1,
) -> io::Result<usize> {
    let provider = normalized_provider(provider)?;
    if ready_state.lifecycle_state != BackendAuthLifecycleState::Ready
        || ready_state.provider != provider
    {
        return Ok(0);
    }
    let continuations_dir = harness_home
        .join("state")
        .join("backend-auth")
        .join("continuations");
    if !continuations_dir.is_dir() {
        return Ok(0);
    }
    let mut files = fs::read_dir(&continuations_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    files.sort();
    let mut resumed = 0usize;
    for file in files {
        let intent: BackendAuthContinuationIntentV1 = serde_json::from_slice(&fs::read(&file)?)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if intent.provider != provider
            || intent.status != BackendAuthContinuationStatus::Waiting
            || ready_state.readiness_generation <= intent.required_newer_than_generation
        {
            continue;
        }
        // The wake record is written first with a deterministic event key. If
        // the process stops before the continuation intent is marked resumed,
        // restart reconciliation observes the existing wake and completes the
        // state transition without appending a duplicate retry-pending row.
        let wake_identity = format!(
            "{}\n{}\n{}\n{}",
            intent.queue_id,
            provider,
            intent.required_newer_than_generation,
            ready_state.readiness_generation
        );
        let wake_digest = digest::digest(&digest::SHA256, wake_identity.as_bytes());
        let wake_event_key = format!("backend-auth-resume:{}", hex_lower(wake_digest.as_ref()));
        let runtime_queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&runtime_queue_dir)?;
        append_jsonl_value_once_by_event_key(
            &runtime_queue_dir.join("run-once-receipts.jsonl"),
            &json!({
                "schema": "agent-harness.runtime-run-once.v1",
                "eventKey": wake_event_key,
                "queueId": intent.queue_id,
                "status": "retry-pending",
                "reason": "backend auth ready; resuming deferred queue exactly once",
                "backendAuthReadinessGeneration": ready_state.readiness_generation,
                "occurredAtMs": current_log_time_ms()?,
            }),
        )?;
        let decision = resume_backend_auth_defer_intent(
            harness_home,
            &intent.queue_id,
            &provider,
            ready_state,
        )?
        .expect("scanned backend auth continuation must remain addressable");
        if !decision.changed {
            continue;
        }
        resumed = resumed.saturating_add(1);
    }
    Ok(resumed)
}

fn classify_account_read(provider: &str, generation: u64, response: &Value) -> BackendAuthStateV1 {
    let Some(result) = response.get("result") else {
        let mut state = new_state(provider, BackendAuthLifecycleState::Failed, generation);
        state.failure_code = Some("account-read-rpc-error".to_string());
        return state;
    };
    let account_present = result
        .get("account")
        .is_some_and(|account| !account.is_null());
    let requires_openai_auth = result.get("requiresOpenaiAuth").and_then(Value::as_bool);
    let lifecycle_state = if account_present {
        BackendAuthLifecycleState::Ready
    } else if requires_openai_auth == Some(true) {
        BackendAuthLifecycleState::Unauthenticated
    } else {
        BackendAuthLifecycleState::RefreshRequired
    };
    let mut state = new_state(provider, lifecycle_state, generation);
    state.requires_openai_auth = requires_openai_auth;
    state
}

fn model_list_capability_ready(response: &Value) -> bool {
    response
        .pointer("/result/data")
        .and_then(Value::as_array)
        .is_some_and(|models| !models.is_empty())
}

fn safe_optional_string(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers.iter().find_map(|pointer| {
        let candidate = value.pointer(pointer)?.as_str()?.trim();
        (!candidate.is_empty()
            && candidate.len() <= 64
            && candidate.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':')
            }))
        .then(|| candidate.to_string())
    })
}

fn backend_auth_lifecycle_label(state: BackendAuthLifecycleState) -> &'static str {
    match state {
        BackendAuthLifecycleState::Unknown => "unknown",
        BackendAuthLifecycleState::Probing => "probing",
        BackendAuthLifecycleState::Unauthenticated => "unauthenticated",
        BackendAuthLifecycleState::LoginPending => "login-pending",
        BackendAuthLifecycleState::Ready => "ready",
        BackendAuthLifecycleState::RefreshRequired => "refresh-required",
        BackendAuthLifecycleState::Failed => "failed",
        BackendAuthLifecycleState::Cancelled => "cancelled",
        BackendAuthLifecycleState::Expired => "expired",
        BackendAuthLifecycleState::LogoutPending => "logout-pending",
    }
}

fn resolve_executable_provenance_receipt_ref(
    receipt_file: Option<&Path>,
    canonical_executable: &Path,
) -> io::Result<Option<String>> {
    let Some(receipt_file) = receipt_file else {
        return Ok(None);
    };
    let bytes = fs::read(receipt_file)?;
    let receipt: crate::codex_backend_provenance::CodexBackendProvenanceReceiptV1 =
        serde_json::from_slice(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if receipt.schema != crate::codex_backend_provenance::CODEX_BACKEND_PROVENANCE_SCHEMA
        || receipt.observed_version
            != crate::codex_backend_provenance::REQUIRED_CODEX_BACKEND_VERSION
        || receipt.probe_result != "ready"
        || receipt.canonical_path.canonicalize()? != canonical_executable
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Codex backend auth provenance receipt does not match the canonical executable",
        ));
    }
    let hash = digest::digest(&digest::SHA256, &bytes);
    Ok(Some(format!("sha256:{}", hex_lower(hash.as_ref()))))
}

fn path_identity_digest(path: &Path) -> String {
    let hash = digest::digest(
        &digest::SHA256,
        path.as_os_str().to_string_lossy().as_bytes(),
    );
    hex_lower(hash.as_ref())
}

fn append_codex_backend_auth_receipt(
    codex_home: &Path,
    receipt: &CodexBackendAuthReceiptV1,
) -> io::Result<()> {
    if receipt.schema != CODEX_BACKEND_AUTH_SCHEMA
        || !is_sha256_hex(&receipt.provider_home_digest)
        || receipt
            .operation_correlation_digest
            .as_deref()
            .is_some_and(|value| !is_sha256_hex(value))
        || !is_safe_auth_label(&receipt.provider, 64)
        || !is_safe_auth_label(&receipt.transition, 64)
        || !is_safe_auth_label(&receipt.selected_method, 64)
        || !is_safe_auth_label(&receipt.account_probe_result, 64)
        || !is_safe_auth_label(&receipt.capability_probe_result, 64)
        || receipt
            .auth_mode
            .as_deref()
            .is_some_and(|value| !is_safe_auth_label(value, 64))
        || receipt
            .plan_type
            .as_deref()
            .is_some_and(|value| !is_safe_auth_label(value, 64))
        || receipt
            .redacted_error_code
            .as_deref()
            .is_some_and(|value| !is_safe_auth_label(value, 96))
        || receipt
            .executable_provenance_receipt_ref
            .as_deref()
            .is_some_and(|value| !value.strip_prefix("sha256:").is_some_and(is_sha256_hex))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid secret-free Codex backend auth receipt",
        ));
    }
    append_jsonl_value(
        &codex_home
            .join(BACKEND_AUTH_STATE_DIR)
            .join(CODEX_BACKEND_AUTH_RECEIPTS_FILE),
        receipt,
    )
}

fn latest_codex_backend_auth_receipt(
    codex_home: &Path,
) -> io::Result<Option<CodexBackendAuthReceiptV1>> {
    let file = codex_home
        .join(BACKEND_AUTH_STATE_DIR)
        .join(CODEX_BACKEND_AUTH_RECEIPTS_FILE);
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(line) = text.lines().rev().find(|line| !line.trim().is_empty()) else {
        return Ok(None);
    };
    let receipt = serde_json::from_str(line)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(Some(receipt))
}

fn new_state(
    provider: &str,
    lifecycle_state: BackendAuthLifecycleState,
    readiness_generation: u64,
) -> BackendAuthStateV1 {
    BackendAuthStateV1 {
        schema: BACKEND_AUTH_STATE_SCHEMA.to_string(),
        provider: provider.to_string(),
        lifecycle_state,
        readiness_generation,
        observed_at_ms: current_log_time_ms().unwrap_or_default(),
        operation_id: None,
        requires_openai_auth: None,
        failure_code: None,
    }
}

fn normalized_provider(provider: &str) -> io::Result<String> {
    let provider = provider.trim().to_ascii_lowercase();
    if provider.is_empty()
        || !provider
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provider must be a non-empty safe path component",
        ));
    }
    Ok(provider)
}

fn nonempty_operation_id(operation_id: &str) -> io::Result<String> {
    let operation_id = operation_id.trim();
    if operation_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backend auth operation id must not be empty",
        ));
    }
    Ok(operation_id.to_string())
}

fn auth_operation_correlation_digest(operation_id: &str) -> String {
    let hash = digest::digest(&digest::SHA256, operation_id.as_bytes());
    hex_lower(hash.as_ref())
}

fn validate_backend_auth_transition(
    current: BackendAuthLifecycleState,
    next: BackendAuthLifecycleState,
) -> io::Result<()> {
    let allowed = match current {
        BackendAuthLifecycleState::Unknown => matches!(
            next,
            BackendAuthLifecycleState::Probing
                | BackendAuthLifecycleState::LoginPending
                | BackendAuthLifecycleState::LogoutPending
        ),
        BackendAuthLifecycleState::Probing => matches!(
            next,
            BackendAuthLifecycleState::Probing
                | BackendAuthLifecycleState::Ready
                | BackendAuthLifecycleState::Unauthenticated
                | BackendAuthLifecycleState::RefreshRequired
                | BackendAuthLifecycleState::Failed
        ),
        BackendAuthLifecycleState::Unauthenticated => matches!(
            next,
            BackendAuthLifecycleState::Probing
                | BackendAuthLifecycleState::LoginPending
                | BackendAuthLifecycleState::LogoutPending
        ),
        BackendAuthLifecycleState::LoginPending => matches!(
            next,
            BackendAuthLifecycleState::Probing
                | BackendAuthLifecycleState::Ready
                | BackendAuthLifecycleState::Failed
                | BackendAuthLifecycleState::Cancelled
                | BackendAuthLifecycleState::Expired
        ),
        BackendAuthLifecycleState::Ready => matches!(
            next,
            BackendAuthLifecycleState::Probing
                | BackendAuthLifecycleState::RefreshRequired
                | BackendAuthLifecycleState::LogoutPending
        ),
        BackendAuthLifecycleState::RefreshRequired
        | BackendAuthLifecycleState::Failed
        | BackendAuthLifecycleState::Cancelled
        | BackendAuthLifecycleState::Expired => matches!(
            next,
            BackendAuthLifecycleState::Probing | BackendAuthLifecycleState::LoginPending
        ),
        BackendAuthLifecycleState::LogoutPending => matches!(
            next,
            BackendAuthLifecycleState::Probing
                | BackendAuthLifecycleState::Unauthenticated
                | BackendAuthLifecycleState::Failed
        ),
    };
    if !allowed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid backend auth transition: {current:?} -> {next:?}"),
        ));
    }
    Ok(())
}

fn backend_auth_state_file(codex_home: &Path) -> PathBuf {
    codex_home
        .join(BACKEND_AUTH_STATE_DIR)
        .join(BACKEND_AUTH_STATE_FILE)
}

fn backend_auth_cancel_file(codex_home: &Path) -> PathBuf {
    codex_home
        .join(BACKEND_AUTH_STATE_DIR)
        .join(BACKEND_AUTH_CANCEL_FILE)
}

fn wait_for_backend_auth_child(
    child: &mut std::process::Child,
    cancel_file: &Path,
    provider: &str,
    operation_correlation_digest: &str,
) -> io::Result<(std::process::ExitStatus, bool)> {
    loop {
        if cancel_file.is_file() {
            let marker: BackendAuthCancelMarker =
                serde_json::from_slice(&fs::read(cancel_file)?)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if marker.schema != BACKEND_AUTH_STATE_SCHEMA
                || marker.provider != provider
                || marker.operation_correlation_digest != operation_correlation_digest
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "backend auth cancel marker does not match the active operation",
                ));
            }
            return terminate_backend_auth_child_process_tree(child).map(|status| (status, true));
        }
        if let Some(status) = child.try_wait()? {
            return Ok((status, false));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn terminate_backend_auth_child_process_tree(
    child: &mut std::process::Child,
) -> io::Result<std::process::ExitStatus> {
    if let Some(status) = child.try_wait()? {
        return Ok(status);
    }
    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", &child.id().to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    let _ = child.kill();
    child.wait()
}

fn backend_auth_continuation_file(harness_home: &Path, queue_id: &str) -> PathBuf {
    let hash = digest::digest(&digest::SHA256, queue_id.as_bytes());
    let file_name = format!("{}.json", hex_lower(hash.as_ref()));
    harness_home
        .join("state")
        .join("backend-auth")
        .join("continuations")
        .join(file_name)
}

fn backend_auth_continuation_receipts_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("backend-auth")
        .join("continuation-receipts.jsonl")
}

fn validate_continuation_identity(
    intent: &BackendAuthContinuationIntentV1,
    queue_id: &str,
    provider: &str,
) -> io::Result<()> {
    if intent.schema != BACKEND_AUTH_CONTINUATION_SCHEMA
        || intent.queue_id != queue_id
        || intent.provider != normalized_provider(provider)?
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backend auth continuation identity mismatch",
        ));
    }
    Ok(())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn validate_secret_free_state(state: &BackendAuthStateV1) -> io::Result<()> {
    let serialized = serde_json::to_string(state).map_err(io::Error::other)?;
    for forbidden in [
        "apiKey",
        "accessToken",
        "refreshToken",
        "deviceCode",
        "authUrl",
    ] {
        if serialized.contains(forbidden) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "backend auth durable state contains a credential-bearing field",
            ));
        }
    }
    if !is_safe_auth_label(&state.provider, 64)
        || state
            .operation_id
            .as_deref()
            .is_some_and(|value| !is_sha256_hex(value))
        || state
            .failure_code
            .as_deref()
            .is_some_and(|value| !is_safe_auth_label(value, 96))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backend auth durable state contains an unsafe diagnostic or correlation value",
        ));
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_safe_auth_label(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "agent-harness-backend-auth-{name}-{}",
            current_log_time_ms().unwrap()
        ))
    }

    #[test]
    fn empty_provider_home_is_unknown_without_global_fallback() {
        let root = temp_root("empty-home");
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        let state = load_backend_auth_state(&home, "openai").unwrap();

        assert_eq!(state.lifecycle_state, BackendAuthLifecycleState::Unknown);
        assert!(home.ends_with("codex-home"));
        assert!(!home.join("auth.json").exists());
        assert!(provider_home_permissions_are_restrictive(&home).unwrap());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn durable_state_is_provider_scoped_and_secret_free() {
        let root = temp_root("state");
        let home = resolve_or_create_provider_codex_home(&root, "openrouter").unwrap();
        let mut state = new_state("openrouter", BackendAuthLifecycleState::LoginPending, 7);
        state.operation_id = Some(auth_operation_correlation_digest("operation-7"));
        persist_backend_auth_state(&home, &state).unwrap();

        let loaded = load_backend_auth_state(&home, "openrouter").unwrap();
        let serialized = fs::read_to_string(backend_auth_state_file(&home)).unwrap();
        assert_eq!(loaded, state);
        assert!(home.ends_with(Path::new("codex-home-providers").join("openrouter")));
        for forbidden in [
            "apiKey",
            "accessToken",
            "refreshToken",
            "deviceCode",
            "authUrl",
        ] {
            assert!(!serialized.contains(forbidden));
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn structured_auth_diagnostics_reject_secret_urls_and_bearer_values() {
        let root = temp_root("structured-redaction-negative");
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        let mut state = new_state("openai", BackendAuthLifecycleState::Failed, 1);
        state.failure_code = Some("accessToken:synthetic-secret".to_string());
        assert!(persist_backend_auth_state(&home, &state).is_err());

        let base = CodexBackendAuthReceiptV1 {
            schema: CODEX_BACKEND_AUTH_SCHEMA.to_string(),
            provider: "openai".to_string(),
            provider_home_digest: "a".repeat(64),
            executable_provenance_receipt_ref: Some(format!("sha256:{}", "b".repeat(64))),
            state: BackendAuthLifecycleState::Failed,
            transition: "operator-operation-completed".to_string(),
            selected_method: "chatgpt-device-code".to_string(),
            operation_correlation_digest: Some("c".repeat(64)),
            started_at_ms: 1,
            completed_at_ms: 2,
            expires_at_ms: None,
            auth_mode: None,
            plan_type: None,
            redacted_error_code: Some("operator-auth-failed".to_string()),
            account_probe_result: "failed".to_string(),
            capability_probe_result: "not-run".to_string(),
            readiness_generation: 1,
        };
        let mut bearer = base.clone();
        bearer.redacted_error_code = Some(format!("{} {}", "Bearer", "synthetic-secret"));
        assert!(append_codex_backend_auth_receipt(&home, &bearer).is_err());
        let mut url = base.clone();
        url.auth_mode = Some(format!(
            "{}{}",
            "https://auth.example/", "authorize?device=secret"
        ));
        assert!(append_codex_backend_auth_receipt(&home, &url).is_err());
        let mut raw_ref = base;
        raw_ref.executable_provenance_receipt_ref =
            Some("D:\\private\\candidate-receipt.json".to_string());
        assert!(append_codex_backend_auth_receipt(&home, &raw_ref).is_err());
        assert!(
            !home
                .join(BACKEND_AUTH_STATE_DIR)
                .join(CODEX_BACKEND_AUTH_RECEIPTS_FILE)
                .exists()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn provider_auth_operation_lease_is_single_writer() {
        let root = temp_root("lease");
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        let first = acquire_backend_auth_operation_lease(&home, "openai", "operation-1").unwrap();
        assert!(acquire_backend_auth_operation_lease(&home, "openai", "operation-2").is_err());
        drop(first);
        assert!(acquire_backend_auth_operation_lease(&home, "openai", "operation-3").is_ok());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn account_probe_participates_in_provider_operation_lease() {
        let root = temp_root("probe-lease");
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        let held = acquire_backend_auth_operation_lease(&home, "openai", "held-operation").unwrap();
        let error = probe_backend_account(BackendAccountProbeOptions {
            harness_home: root.clone(),
            provider: "openai".to_string(),
            codex_executable: root.join("must-not-be-inspected-while-lease-held"),
            executable_provenance_receipt: None,
            timeout: Duration::from_millis(1),
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        drop(held);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dead_owner_auth_operation_lease_is_restart_reclaimable() {
        let root = temp_root("stale-lease");
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        let state_dir = home.join(BACKEND_AUTH_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        write_json_atomic(
            &state_dir.join(BACKEND_AUTH_LEASE_FILE),
            &BackendAuthLeaseRecord {
                schema: BACKEND_AUTH_STATE_SCHEMA.to_string(),
                provider: "openai".to_string(),
                operation_id: "crashed-operation".to_string(),
                owner_pid: i64::MAX,
                acquired_at_ms: 1,
            },
        )
        .unwrap();

        assert!(acquire_backend_auth_operation_lease(&home, "openai", "restart-operation").is_ok());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn readiness_requires_fresh_ready_generation() {
        let state = new_state("openai", BackendAuthLifecycleState::Ready, 4);
        assert!(require_backend_ready_for_turn(&state, 4).ready);
        assert_eq!(
            require_backend_ready_for_turn(&state, 3)
                .defer_reason
                .as_deref(),
            Some("stale-auth-readiness-generation")
        );
        let unauthenticated = new_state("openai", BackendAuthLifecycleState::Unauthenticated, 5);
        assert_eq!(
            require_backend_ready_for_turn(&unauthenticated, 5)
                .defer_reason
                .as_deref(),
            Some("needs-operator-auth")
        );
    }

    #[test]
    fn account_read_fixture_classifies_empty_home_as_unauthenticated() {
        let response = json!({"id":1,"result":{"account":null,"requiresOpenaiAuth":true}});
        let state = classify_account_read("openai", 1, &response);
        assert_eq!(
            state.lifecycle_state,
            BackendAuthLifecycleState::Unauthenticated
        );
        assert_eq!(state.requires_openai_auth, Some(true));
    }

    #[test]
    fn account_probe_requires_model_capability_and_writes_secret_free_receipt() {
        let root = temp_root("account-capability-receipt");
        let executable = fake_account_probe_executable(&root);
        let state = probe_backend_account(BackendAccountProbeOptions {
            harness_home: root.clone(),
            provider: "openai".to_string(),
            codex_executable: executable,
            executable_provenance_receipt: None,
            timeout: Duration::from_secs(10),
        })
        .unwrap();
        assert_eq!(state.lifecycle_state, BackendAuthLifecycleState::Ready);
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        let receipt_file = home
            .join(BACKEND_AUTH_STATE_DIR)
            .join(CODEX_BACKEND_AUTH_RECEIPTS_FILE);
        let receipts = fs::read_to_string(&receipt_file).unwrap();
        let receipt: CodexBackendAuthReceiptV1 =
            serde_json::from_str(receipts.lines().last().unwrap()).unwrap();
        assert_eq!(receipt.schema, CODEX_BACKEND_AUTH_SCHEMA);
        assert_eq!(receipt.account_probe_result, "ready");
        assert_eq!(receipt.capability_probe_result, "ready");
        assert_eq!(receipt.auth_mode.as_deref(), Some("chatgpt"));
        assert_eq!(receipt.plan_type.as_deref(), Some("team"));
        assert_eq!(
            receipt
                .operation_correlation_digest
                .as_deref()
                .unwrap()
                .len(),
            64
        );
        assert!(receipt.executable_provenance_receipt_ref.is_none());
        for forbidden in [
            "accessToken",
            "refreshToken",
            "idToken",
            "apiKey",
            "deviceCode",
            "authUrl",
            "account@example",
        ] {
            assert!(!receipts.contains(forbidden));
        }
        let doctor = doctor_backend_auth(&root, "openai").unwrap();
        assert!(doctor.ready);
        assert!(doctor.issues.is_empty());
        assert_eq!(doctor.latest_account_probe_result.as_deref(), Some("ready"));
        assert_eq!(
            doctor.latest_capability_probe_result.as_deref(),
            Some("ready")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ready_account_without_model_capability_is_not_runtime_ready() {
        let response = json!({"id":2,"result":{"data":[]}});
        assert!(!model_list_capability_ready(&response));
        let response = json!({"id":2,"error":{"message":"not available"}});
        assert!(!model_list_capability_ready(&response));
    }

    #[test]
    fn operator_methods_use_expected_cli_contract_and_never_persist_api_key() {
        let cases = [
            (BackendAuthCliAction::BrowserLogin, "chatgpt-browser", true),
            (
                BackendAuthCliAction::DeviceCodeLogin,
                "chatgpt-device-code",
                true,
            ),
            (
                BackendAuthCliAction::ApiKeyStdinLogin,
                "api-key-stdin",
                true,
            ),
            (BackendAuthCliAction::Logout, "logout", false),
        ];
        for (action, expected_method, expected_ready) in cases {
            let root = temp_root(&format!("operator-method-{expected_method}"));
            let executable = fake_operator_auth_executable(&root);
            let secret = b"synthetic-api-key-must-not-persist".to_vec();
            let state = run_backend_auth_cli_operation(BackendAuthCliOperationOptions {
                harness_home: root.clone(),
                provider: "openai".to_string(),
                codex_executable: executable,
                executable_provenance_receipt: None,
                action,
                api_key_stdin: (action == BackendAuthCliAction::ApiKeyStdinLogin)
                    .then_some(secret.clone()),
                probe_timeout: Duration::from_secs(10),
            })
            .unwrap();
            assert_eq!(
                state.lifecycle_state == BackendAuthLifecycleState::Ready,
                expected_ready
            );
            let call_log = fs::read_to_string(root.join("operator-call.txt")).unwrap();
            assert_eq!(call_log.trim(), expected_method);
            assert!(!call_log.contains(std::str::from_utf8(&secret).unwrap()));
            let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
            let receipts = fs::read_to_string(
                home.join(BACKEND_AUTH_STATE_DIR)
                    .join(CODEX_BACKEND_AUTH_RECEIPTS_FILE),
            )
            .unwrap();
            assert!(receipts.contains(&format!(r#""selectedMethod":"{expected_method}""#)));
            assert!(!receipts.contains(std::str::from_utf8(&secret).unwrap()));
            assert!(
                !fs::read_to_string(backend_auth_state_file(&home))
                    .unwrap()
                    .contains(std::str::from_utf8(&secret).unwrap())
            );
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn lifecycle_completion_requires_exact_operation_correlation() {
        let root = temp_root("correlation");
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        begin_backend_auth_operation(
            &home,
            "openai",
            "operation-1",
            BackendAuthLifecycleState::LoginPending,
        )
        .unwrap();

        let wrong = complete_backend_auth_operation(
            &home,
            "openai",
            BackendAuthTransition {
                operation_id: "operation-other".to_string(),
                next_state: BackendAuthLifecycleState::Ready,
                requires_openai_auth: Some(false),
                failure_code: None,
            },
        );
        assert!(wrong.is_err());
        let completed = complete_backend_auth_operation(
            &home,
            "openai",
            BackendAuthTransition {
                operation_id: "operation-1".to_string(),
                next_state: BackendAuthLifecycleState::Ready,
                requires_openai_auth: Some(false),
                failure_code: None,
            },
        )
        .unwrap();
        assert_eq!(completed.lifecycle_state, BackendAuthLifecycleState::Ready);
        assert_eq!(completed.operation_id, None);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn lifecycle_replay_rejects_illegal_transition_and_supports_expiry() {
        let root = temp_root("transition-replay");
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        assert!(
            begin_backend_auth_operation(
                &home,
                "openai",
                "operation-ready",
                BackendAuthLifecycleState::Ready,
            )
            .is_err()
        );
        begin_backend_auth_operation(
            &home,
            "openai",
            "operation-login",
            BackendAuthLifecycleState::LoginPending,
        )
        .unwrap();
        let expired = expire_backend_auth_operation(&home, "openai", "operation-login").unwrap();
        assert_eq!(expired.lifecycle_state, BackendAuthLifecycleState::Expired);
        assert_eq!(
            expired.failure_code.as_deref(),
            Some("operator-auth-expired")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn restart_reconciles_orphaned_login_pending_through_account_and_capability_probe() {
        let root = temp_root("restart-orphaned-login");
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        begin_backend_auth_operation(
            &home,
            "openai",
            "login-before-process-stop",
            BackendAuthLifecycleState::LoginPending,
        )
        .unwrap();
        let before = doctor_backend_auth(&root, "openai").unwrap();
        assert!(!before.ready);
        assert!(
            before
                .issues
                .iter()
                .any(|issue| issue.contains("no live provider operation lease"))
        );

        let reconciled = probe_backend_account(BackendAccountProbeOptions {
            harness_home: root.clone(),
            provider: "openai".to_string(),
            codex_executable: fake_account_probe_executable(&root),
            executable_provenance_receipt: None,
            timeout: Duration::from_secs(10),
        })
        .unwrap();
        assert_eq!(reconciled.lifecycle_state, BackendAuthLifecycleState::Ready);
        let after = doctor_backend_auth(&root, "openai").unwrap();
        assert!(after.ready);
        assert!(after.issues.is_empty());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn operator_cancel_is_exact_idempotent_and_stores_only_correlation_digest() {
        let root = temp_root("operator-cancel");
        let home = resolve_or_create_provider_codex_home(&root, "openai").unwrap();
        let operation_id = "raw-login-operation-must-not-persist";
        let pending = begin_backend_auth_operation(
            &home,
            "openai",
            operation_id,
            BackendAuthLifecycleState::LoginPending,
        )
        .unwrap();
        assert_ne!(pending.operation_id.as_deref(), Some(operation_id));
        assert_eq!(pending.operation_id.as_deref().unwrap().len(), 64);

        let first = request_backend_auth_cancel(&root, "openai").unwrap();
        let duplicate = request_backend_auth_cancel(&root, "openai").unwrap();
        assert!(first.changed);
        assert!(!duplicate.changed);
        assert_eq!(
            first.operation_correlation_digest,
            duplicate.operation_correlation_digest
        );
        let marker_text = fs::read_to_string(backend_auth_cancel_file(&home)).unwrap();
        assert!(!marker_text.contains(operation_id));

        #[cfg(windows)]
        let mut child = Command::new("powershell")
            .args(["-NoProfile", "-Command", "Start-Sleep -Seconds 30"])
            .spawn()
            .unwrap();
        #[cfg(not(windows))]
        let mut child = Command::new("sh").args(["-c", "sleep 30"]).spawn().unwrap();
        let (_, cancelled) = wait_for_backend_auth_child(
            &mut child,
            &backend_auth_cancel_file(&home),
            "openai",
            &first.operation_correlation_digest,
        )
        .unwrap();
        assert!(cancelled);
        let completed = complete_backend_auth_operation(
            &home,
            "openai",
            BackendAuthTransition {
                operation_id: operation_id.to_string(),
                next_state: BackendAuthLifecycleState::Cancelled,
                requires_openai_auth: None,
                failure_code: Some("operator-auth-cancelled".to_string()),
            },
        )
        .unwrap();
        assert_eq!(
            completed.lifecycle_state,
            BackendAuthLifecycleState::Cancelled
        );
        assert_eq!(completed.operation_id, None);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn auth_defer_and_newer_ready_resume_are_exactly_once() {
        let root = temp_root("defer-resume");
        let harness_home = root.join("harness");
        let first =
            record_backend_auth_defer_intent(&harness_home, "queue:auth:1", "openai", 3).unwrap();
        let duplicate =
            record_backend_auth_defer_intent(&harness_home, "queue:auth:1", "openai", 3).unwrap();
        assert!(first.changed);
        assert!(!duplicate.changed);

        let stale_ready = new_state("openai", BackendAuthLifecycleState::Ready, 3);
        assert!(
            !resume_backend_auth_defer_intent(
                &harness_home,
                "queue:auth:1",
                "openai",
                &stale_ready,
            )
            .unwrap()
            .unwrap()
            .changed
        );
        let fresh_ready = new_state("openai", BackendAuthLifecycleState::Ready, 4);
        let resumed =
            resume_backend_auth_defer_intent(&harness_home, "queue:auth:1", "openai", &fresh_ready)
                .unwrap()
                .unwrap();
        let duplicate_resume =
            resume_backend_auth_defer_intent(&harness_home, "queue:auth:1", "openai", &fresh_ready)
                .unwrap()
                .unwrap();
        assert!(resumed.changed);
        assert_eq!(resumed.intent.resume_count, 1);
        assert!(!duplicate_resume.changed);

        let receipts =
            fs::read_to_string(backend_auth_continuation_receipts_file(&harness_home)).unwrap();
        assert_eq!(receipts.lines().count(), 2);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ready_reconciliation_wakes_each_waiting_queue_once() {
        let root = temp_root("resume-all");
        let harness_home = root.join("harness");
        for queue_id in ["queue:auth:a", "queue:auth:b"] {
            record_backend_auth_defer_intent(&harness_home, queue_id, "openai", 2).unwrap();
        }
        let ready = new_state("openai", BackendAuthLifecycleState::Ready, 3);
        assert_eq!(
            resume_all_backend_auth_defer_intents(&harness_home, "openai", &ready).unwrap(),
            2
        );
        assert_eq!(
            resume_all_backend_auth_defer_intents(&harness_home, "openai", &ready).unwrap(),
            0
        );
        let receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .unwrap();
        assert_eq!(receipts.lines().count(), 2);
        assert!(receipts.lines().all(|line| line.contains("retry-pending")));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ready_reconciliation_recovers_after_wake_append_before_intent_mark() {
        let root = temp_root("resume-crash-window");
        let harness_home = root.join("harness");
        let queue_id = "queue:auth:crash-window";
        record_backend_auth_defer_intent(&harness_home, queue_id, "openai", 8).unwrap();
        let ready = new_state("openai", BackendAuthLifecycleState::Ready, 9);
        let wake_identity = format!("{queue_id}\nopenai\n8\n9");
        let wake_digest = digest::digest(&digest::SHA256, wake_identity.as_bytes());
        let event_key = format!("backend-auth-resume:{}", hex_lower(wake_digest.as_ref()));
        let receipts_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("run-once-receipts.jsonl");
        append_jsonl_value_once_by_event_key(
            &receipts_file,
            &json!({
                "schema": "agent-harness.runtime-run-once.v1",
                "eventKey": event_key,
                "queueId": queue_id,
                "status": "retry-pending",
                "reason": "simulated durable wake before process stop",
                "backendAuthReadinessGeneration": 9,
                "occurredAtMs": 1,
            }),
        )
        .unwrap();

        assert_eq!(
            resume_all_backend_auth_defer_intents(&harness_home, "openai", &ready).unwrap(),
            1
        );
        assert_eq!(
            resume_all_backend_auth_defer_intents(&harness_home, "openai", &ready).unwrap(),
            0
        );
        assert_eq!(
            fs::read_to_string(&receipts_file).unwrap().lines().count(),
            1
        );
        let decision = resume_backend_auth_defer_intent(&harness_home, queue_id, "openai", &ready)
            .unwrap()
            .unwrap();
        assert!(!decision.changed);
        assert_eq!(
            decision.intent.status,
            BackendAuthContinuationStatus::Resumed
        );
        assert_eq!(decision.intent.resume_count, 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_auth_gate_is_explicit_and_default_off() {
        let root = temp_root("runtime-gate");
        assert!(!backend_auth_runtime_gate_enabled(&root).unwrap());
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("harness-config.json"),
            r#"{"backendAuth":{"runtimeGateEnabled":true}}"#,
        )
        .unwrap();
        assert!(backend_auth_runtime_gate_enabled(&root).unwrap());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    fn fake_account_probe_executable(root: &Path) -> PathBuf {
        fs::create_dir_all(root).unwrap();
        let script = root.join("fake-account-probe.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
    } elseif ($msg.id -eq 1) {
        [Console]::Out.WriteLine('{"id":1,"result":{"account":{"type":"chatgpt"},"requiresOpenaiAuth":true,"authMode":"chatgpt","planType":"team"}}')
    } elseif ($msg.id -eq 2) {
        [Console]::Out.WriteLine('{"id":2,"result":{"data":[{"id":"gpt-fixture"}],"nextCursor":null}}')
    }
    [Console]::Out.Flush()
}
"#,
        )
        .unwrap();
        let command = root.join("fake-account-probe.cmd");
        fs::write(
            &command,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        command
    }

    #[cfg(not(windows))]
    fn fake_account_probe_executable(root: &Path) -> PathBuf {
        fs::create_dir_all(root).unwrap();
        let script = root.join("fake-account-probe");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*) printf '%s\n' '{"id":0,"result":{"ok":true}}' ;;
        *'"id":1'*) printf '%s\n' '{"id":1,"result":{"account":{"type":"chatgpt"},"requiresOpenaiAuth":true,"authMode":"chatgpt","planType":"team"}}' ;;
        *'"id":2'*) printf '%s\n' '{"id":2,"result":{"data":[{"id":"gpt-fixture"}],"nextCursor":null}}' ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(windows)]
    fn fake_operator_auth_executable(root: &Path) -> PathBuf {
        fs::create_dir_all(root).unwrap();
        let script = root.join("fake-operator-auth.ps1");
        let call_file = root
            .join("operator-call.txt")
            .display()
            .to_string()
            .replace('\'', "''");
        let mode_file = root
            .join("operator-mode.txt")
            .display()
            .to_string()
            .replace('\'', "''");
        fs::write(
            &script,
            format!(
                r#"
$callFile = '{call_file}'
$modeFile = '{mode_file}'
$loggedOutResponse = $null
if ($args.Count -gt 0 -and $args[0] -eq 'login') {{
    if ($args -contains '--device-auth') {{ $method = 'chatgpt-device-code' }}
    elseif ($args -contains '--with-api-key') {{
        $method = 'api-key-stdin'
        # The production CLI contract sends exactly one newline-terminated
        # secret. Read one line so the cmd.exe test wrapper cannot keep its
        # inherited stdin handle open and prevent EOF forever.
        $null = [Console]::In.ReadLine()
    }} else {{ $method = 'chatgpt-browser' }}
    Set-Content -LiteralPath $callFile -NoNewline -Value $method
    Set-Content -LiteralPath $modeFile -NoNewline -Value 'ready'
    exit 0
}}
if ($args.Count -gt 0 -and $args[0] -eq 'logout') {{
    Set-Content -LiteralPath $callFile -NoNewline -Value 'logout'
    Set-Content -LiteralPath $modeFile -NoNewline -Value 'logged-out'
    exit 0
}}
while ($true) {{
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) {{ break }}
    try {{ $msg = $line | ConvertFrom-Json }} catch {{ continue }}
    if ($msg.id -eq 0) {{
        [Console]::Out.WriteLine('{{"id":0,"result":{{"ok":true}}}}')
    }} elseif ($msg.id -eq 1) {{
        $mode = if (Test-Path -LiteralPath $modeFile) {{ Get-Content -LiteralPath $modeFile -Raw }} else {{ 'logged-out' }}
        if ($mode -eq 'ready') {{
            [Console]::Out.WriteLine('{{"id":1,"result":{{"account":{{"type":"chatgpt"}},"requiresOpenaiAuth":true,"authMode":"chatgpt","planType":"team"}}}}')
        }} else {{
            # Keep the Windows wrapper alive until model/list is consumed. If
            # account/read closes the probe first, killing cmd.exe can leave
            # its PowerShell descendant holding the inherited stdout pipe.
            $loggedOutResponse = '{{"id":1,"result":{{"account":null,"requiresOpenaiAuth":true}}}}'
        }}
    }} elseif ($msg.id -eq 2) {{
        [Console]::Out.WriteLine('{{"id":2,"result":{{"data":[{{"id":"gpt-fixture"}}],"nextCursor":null}}}}')
        if ($null -ne $loggedOutResponse) {{
            [Console]::Out.WriteLine($loggedOutResponse)
        }}
        [Console]::Out.Flush()
        exit 0
    }}
    [Console]::Out.Flush()
}}
"#
            ),
        )
        .unwrap();
        let command = root.join("fake-operator-auth.cmd");
        fs::write(
            &command,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\" %*\r\n",
                script.display()
            ),
        )
        .unwrap();
        command
    }

    #[cfg(not(windows))]
    fn fake_operator_auth_executable(root: &Path) -> PathBuf {
        fs::create_dir_all(root).unwrap();
        let script = root.join("fake-operator-auth");
        fs::write(
            &script,
            r#"#!/bin/sh
call_file="$(dirname "$0")/operator-call.txt"
mode_file="$(dirname "$0")/operator-mode.txt"
if [ "$1" = "login" ]; then
    method="chatgpt-browser"
    for arg in "$@"; do
        [ "$arg" = "--device-auth" ] && method="chatgpt-device-code"
        if [ "$arg" = "--with-api-key" ]; then method="api-key-stdin"; cat >/dev/null; fi
    done
    printf '%s' "$method" > "$call_file"
    printf '%s' ready > "$mode_file"
    exit 0
fi
if [ "$1" = "logout" ]; then
    printf '%s' logout > "$call_file"
    printf '%s' logged-out > "$mode_file"
    exit 0
fi
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*) printf '%s\n' '{"id":0,"result":{"ok":true}}' ;;
        *'"id":1'*)
            mode="$(cat "$mode_file" 2>/dev/null || printf logged-out)"
            if [ "$mode" = ready ]; then
                printf '%s\n' '{"id":1,"result":{"account":{"type":"chatgpt"},"requiresOpenaiAuth":true,"authMode":"chatgpt","planType":"team"}}'
            else
                printf '%s\n' '{"id":1,"result":{"account":null,"requiresOpenaiAuth":true}}'
            fi ;;
        *'"id":2'*) printf '%s\n' '{"id":2,"result":{"data":[{"id":"gpt-fixture"}],"nextCursor":null}}' ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }
}
