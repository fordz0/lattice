use crate::site_helpers::validate_name;
use lattice_site::manifest::{validate_app_manifest, AppManifest};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const MAX_REGISTERED_APPS: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalAppRegistration {
    pub site_name: String,
    pub proxy_port: u16,
    pub proxy_paths: Vec<String>,
    pub registered_at: u64,
    pub pid: u32,
}

#[derive(Clone, Default)]
pub struct AppRegistry {
    inner: Arc<Mutex<HashMap<String, LocalAppRegistration>>>,
}

impl AppRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, reg: LocalAppRegistration) -> Result<(), String> {
        validate_name(&reg.site_name)?;
        validate_app_manifest(&AppManifest {
            proxy_port: reg.proxy_port,
            proxy_paths: reg.proxy_paths.clone(),
        })?;

        let mut guard = self
            .inner
            .lock()
            .map_err(|_| "app registry mutex poisoned".to_string())?;

        if let Some(existing) = guard.get(&reg.site_name) {
            if existing.pid != reg.pid && pid_is_alive(existing.pid) {
                return Err("site already registered by another live process".to_string());
            }
        } else if guard.len() >= MAX_REGISTERED_APPS {
            return Err("too many registered local apps".to_string());
        }

        guard.insert(reg.site_name.clone(), reg);
        Ok(())
    }

    pub fn unregister(&self, site_name: &str, pid: u32) -> Result<(), String> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| "app registry mutex poisoned".to_string())?;
        let Some(existing) = guard.get(site_name) else {
            return Err("app not registered".to_string());
        };
        if existing.pid != pid {
            return Err("app registration pid mismatch".to_string());
        }
        guard.remove(site_name);
        Ok(())
    }

    pub fn get(&self, site_name: &str) -> Option<LocalAppRegistration> {
        self.inner
            .lock()
            .ok()
            .and_then(|guard| guard.get(site_name).cloned())
    }

    pub fn list(&self) -> Vec<LocalAppRegistration> {
        self.inner
            .lock()
            .map(|guard| guard.values().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(unix)]
pub fn pid_is_alive(pid: u32) -> bool {
    unsafe {
        if libc::kill(pid as libc::pid_t, 0) == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
}

#[cfg(windows)]
pub fn pid_is_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }

        let mut exit_code = 0;
        let ok = GetExitCodeProcess(handle, &mut exit_code);
        let _ = CloseHandle(handle);
        ok != 0 && exit_code == STILL_ACTIVE
    }
}
