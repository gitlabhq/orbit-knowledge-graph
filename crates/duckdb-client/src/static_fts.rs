#![allow(
    unsafe_code,
    reason = "DuckDB exposes database ownership and extension registration only through its C API"
)]

use std::ffi::{CStr, CString, c_void};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

use crate::error::{DuckDbError, Result};

pub(crate) struct OwnedDatabase(libduckdb_sys::duckdb_database);

// DuckDB permits its connection and database handles to move together between threads.
unsafe impl Send for OwnedDatabase {}

impl Drop for OwnedDatabase {
    fn drop(&mut self) {
        unsafe { libduckdb_sys::duckdb_close(&mut self.0) };
    }
}

struct OwnedConfig(libduckdb_sys::duckdb_config);

impl Drop for OwnedConfig {
    fn drop(&mut self) {
        unsafe { libduckdb_sys::duckdb_destroy_config(&mut self.0) };
    }
}

unsafe extern "C" {
    fn orbit_duckdb_load_fts(
        database: libduckdb_sys::duckdb_database,
        out_error: *mut *mut std::ffi::c_char,
    ) -> std::ffi::c_int;
}

pub(crate) fn open(
    path: &Path,
    access_mode: duckdb::AccessMode,
) -> Result<(duckdb::Connection, OwnedDatabase)> {
    let path = path_to_cstring(path)?;
    let access_mode = CString::new(access_mode.to_string()).unwrap();
    let mut config = std::ptr::null_mut();
    let mut database = std::ptr::null_mut();
    let mut error = std::ptr::null_mut();

    unsafe {
        if libduckdb_sys::duckdb_create_config(&mut config) != libduckdb_sys::DuckDBSuccess {
            libduckdb_sys::duckdb_destroy_config(&mut config);
            return Err(DuckDbError::Schema(
                "failed to create DuckDB configuration".to_string(),
            ));
        }
        let config = OwnedConfig(config);
        if libduckdb_sys::duckdb_set_config(config.0, c"access_mode".as_ptr(), access_mode.as_ptr())
            != libduckdb_sys::DuckDBSuccess
        {
            return Err(DuckDbError::Schema(
                "failed to configure DuckDB access mode".to_string(),
            ));
        }
        if libduckdb_sys::duckdb_set_config(config.0, c"duckdb_api".as_ptr(), c"rust".as_ptr())
            != libduckdb_sys::DuckDBSuccess
        {
            return Err(DuckDbError::Schema(
                "failed to configure the DuckDB API identifier".to_string(),
            ));
        }

        let state =
            libduckdb_sys::duckdb_open_ext(path.as_ptr(), &mut database, config.0, &mut error);
        let message = take_error(error);
        if state != libduckdb_sys::DuckDBSuccess {
            if !database.is_null() {
                libduckdb_sys::duckdb_close(&mut database);
            }
            return Err(DuckDbError::Schema(
                message.unwrap_or_else(|| "DuckDB open failed".to_string()),
            ));
        }

        let database = OwnedDatabase(database);
        let mut extension_error = std::ptr::null_mut();
        if orbit_duckdb_load_fts(database.0, &mut extension_error) != 0 {
            let message =
                take_error(extension_error).unwrap_or_else(|| "unknown C++ exception".to_string());
            return Err(DuckDbError::Schema(format!(
                "failed to register the static DuckDB fts extension: {message}"
            )));
        }
        let conn = duckdb::Connection::open_from_raw(database.0)?;
        Ok((conn, database))
    }
}

unsafe fn take_error(error: *mut std::ffi::c_char) -> Option<String> {
    if error.is_null() {
        return None;
    }
    let message = unsafe { CStr::from_ptr(error) }
        .to_string_lossy()
        .into_owned();
    unsafe { libduckdb_sys::duckdb_free(error.cast::<c_void>()) };
    Some(message)
}

#[cfg(unix)]
fn path_to_cstring(path: &Path) -> Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|error| DuckDbError::Schema(error.to_string()))
}

#[cfg(not(unix))]
fn path_to_cstring(path: &Path) -> Result<CString> {
    let path = path
        .to_str()
        .ok_or_else(|| DuckDbError::Schema(format!("invalid database path: {}", path.display())))?;
    CString::new(path).map_err(|error| DuckDbError::Schema(error.to_string()))
}
