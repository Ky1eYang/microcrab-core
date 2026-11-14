//! SQLite connection setup shared by metadata and content-addressed history.
//! Callers own the connection in a blocking worker; no lock crosses an async IO.
use microcrab_session::Error;
use rusqlite::{Connection, OptionalExtension};
use std::{path::Path, time::Duration};

#[cfg(test)]
#[path = "sqlite/tests.rs"]
mod tests;

pub(crate) fn failure(error: rusqlite::Error) -> Error {
    use microcrab_protocol::diagnostic::{describe, json_reason};
    use rusqlite::Error as E;
    let description = match &error {
        E::SqliteFailure(native, message) => format!(
            "native_code={:?}; extended_code={}; {}",
            native.code,
            native.extended_code,
            message
                .as_ref()
                .map(|message| describe(message, 1600))
                .unwrap_or_else(|| describe(native, 1600))
        ),
        E::FromSqlConversionFailure(index, kind, source) => {
            let cause = source
                .downcast_ref::<serde_json::Error>()
                .map(json_reason)
                .unwrap_or_else(|| "stored value cannot convert to requested Rust type".to_owned());
            format!("column={index}; stored_type={kind}; {cause}")
        }
        E::IntegralValueOutOfRange(index, _) => {
            format!("integer outside requested Rust range; column={index}")
        }
        E::InvalidParameterName(_) => {
            "bound parameter name does not exist in the statement".to_owned()
        }
        E::InvalidColumnName(_) => "requested column name does not exist in the result".to_owned(),
        E::InvalidColumnType(index, _, kind) => {
            format!("column={index}; stored_type={kind}; requested Rust type is incompatible")
        }
        E::ToSqlConversionFailure(source) => source
            .downcast_ref::<serde_json::Error>()
            .map(json_reason)
            .unwrap_or_else(|| "value cannot convert to a SQLite parameter".to_owned()),
        E::SqliteSingleThreadedMode
        | E::Utf8Error(_)
        | E::NulError(_)
        | E::InvalidPath(_)
        | E::ExecuteReturnedResults
        | E::QueryReturnedNoRows
        | E::InvalidColumnIndex(_)
        | E::StatementChangedRows(_)
        | E::InvalidQuery
        | E::UnwindingPanic
        | E::MultipleStatement
        | E::InvalidParameterCount(..) => describe(&error, 1800),
        _ => {
            // Optional/future native SQL-input errors can embed the entire SQL in
            // Display. Their native source remains safe without that statement.
            std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<rusqlite::ffi::Error>())
                .map(|native| {
                    format!(
                        "native_code={:?}; extended_code={}",
                        native.code, native.extended_code
                    )
                })
                .unwrap_or_else(|| {
                    "SQLite extension failed without a safe native diagnostic".to_owned()
                })
        }
    };
    Error::Protocol(format!("SQLite storage failure: {description}").into())
}

pub(crate) fn open(path: &Path, component: &str) -> Result<Connection, Error> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path).map_err(failure)?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(failure)?;
    connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS microcrab_schema(component TEXT PRIMARY KEY, version INTEGER NOT NULL);")
        .map_err(failure)?;
    let version: Option<i64> = connection
        .query_row(
            "SELECT version FROM microcrab_schema WHERE component=?1",
            [component],
            |row| row.get(0),
        )
        .optional()
        .map_err(failure)?;
    match version {
        Some(1) => {}
        Some(_) => return Err(Error::config("unsupported SQLite storage schema")),
        None => {
            connection
                .execute(
                    "INSERT INTO microcrab_schema(component,version) VALUES(?1,1)",
                    [component],
                )
                .map_err(failure)?;
        }
    }
    Ok(connection)
}
