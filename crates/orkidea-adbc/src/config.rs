// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::time::Duration;

use adbc_core::error::Status;
use adbc_core::options::OptionValue;

use crate::error::{error, expect_bool, expect_int, expect_string, from_oracle, Result};
use crate::types::{TypeOptions, UnconstrainedNumber};

/// Validates `oracle.unconstrained_number_scale` (0..=38, the Decimal128 limit).
pub(crate) fn validate_number_scale(scale: i64) -> Result<i8> {
    if (0..=38).contains(&scale) {
        Ok(scale as i8)
    } else {
        Err(error(
            format!("{OPT_UNCONSTRAINED_NUMBER_SCALE} must be between 0 and 38, got {scale}"),
            Status::InvalidArguments,
        ))
    }
}

pub(crate) const OPT_URI: &str = "uri";
pub(crate) const OPT_USERNAME: &str = "username";
pub(crate) const OPT_PASSWORD: &str = "password";
pub(crate) const OPT_CONNECT_STRING: &str = "oracle.connect_string";
pub(crate) const OPT_CONFIG_DIR: &str = "oracle.config_dir";
pub(crate) const OPT_WALLET_LOCATION: &str = "oracle.wallet_location";
pub(crate) const OPT_WALLET_PASSWORD: &str = "oracle.wallet_password";
pub(crate) const OPT_STMT_CACHE_SIZE: &str = "oracle.stmt_cache_size";
pub(crate) const OPT_CONNECT_TIMEOUT_MS: &str = "oracle.connect_timeout_ms";
pub(crate) const OPT_CALL_TIMEOUT_MS: &str = "oracle.call_timeout_ms";
pub(crate) const OPT_PROGRAM: &str = "oracle.program";
pub(crate) const OPT_MACHINE: &str = "oracle.machine";
pub(crate) const OPT_TERMINAL: &str = "oracle.terminal";
pub(crate) const OPT_CCLASS: &str = "oracle.cclass";
pub(crate) const OPT_BATCH_SIZE: &str = "oracle.batch_size";
pub(crate) const OPT_FETCH_ARRAY_SIZE: &str = "oracle.fetch_array_size";
pub(crate) const OPT_PREFETCH_ROWS: &str = "oracle.prefetch_rows";
pub(crate) const OPT_AUTH_MODE: &str = "oracle.auth_mode";
pub(crate) const OPT_NEW_PASSWORD: &str = "oracle.new_password";
pub(crate) const OPT_EDITION: &str = "oracle.edition";
pub(crate) const OPT_TIME_ZONE: &str = "oracle.time_zone";
pub(crate) const OPT_MODULE: &str = "oracle.module";
pub(crate) const OPT_ACTION: &str = "oracle.action";
pub(crate) const OPT_CLIENT_IDENTIFIER: &str = "oracle.client_identifier";
pub(crate) const OPT_CLIENT_INFO: &str = "oracle.client_info";
pub(crate) const OPT_DBOP: &str = "oracle.dbop";
pub(crate) const OPT_UNCONSTRAINED_NUMBER: &str = "oracle.unconstrained_number";
pub(crate) const OPT_UNCONSTRAINED_NUMBER_SCALE: &str = "oracle.unconstrained_number_scale";
pub(crate) const OPT_COLUMNAR_FETCH: &str = "oracle.columnar_fetch";

const KNOWN_OPTIONS: &[&str] = &[
    OPT_URI,
    OPT_USERNAME,
    OPT_PASSWORD,
    OPT_CONNECT_STRING,
    OPT_CONFIG_DIR,
    OPT_WALLET_LOCATION,
    OPT_WALLET_PASSWORD,
    OPT_STMT_CACHE_SIZE,
    OPT_CONNECT_TIMEOUT_MS,
    OPT_CALL_TIMEOUT_MS,
    OPT_PROGRAM,
    OPT_MACHINE,
    OPT_TERMINAL,
    OPT_CCLASS,
    OPT_BATCH_SIZE,
    OPT_FETCH_ARRAY_SIZE,
    OPT_PREFETCH_ROWS,
    OPT_UNCONSTRAINED_NUMBER,
    OPT_UNCONSTRAINED_NUMBER_SCALE,
    OPT_COLUMNAR_FETCH,
    OPT_AUTH_MODE,
    OPT_NEW_PASSWORD,
    OPT_EDITION,
    OPT_TIME_ZONE,
    OPT_MODULE,
    OPT_ACTION,
    OPT_CLIENT_IDENTIFIER,
    OPT_CLIENT_INFO,
    OPT_DBOP,
];

/// Options whose values are never returned by `get_option` or echoed in
/// error messages.
pub(crate) const SECRET_OPTIONS: &[&str] = &[OPT_PASSWORD, OPT_WALLET_PASSWORD, OPT_NEW_PASSWORD];

/// Tracing attributes, settable on the database (applied to every new
/// connection) and on a connection.
pub(crate) const TRACING_OPTIONS: &[&str] = &[
    OPT_MODULE,
    OPT_ACTION,
    OPT_CLIENT_IDENTIFIER,
    OPT_CLIENT_INFO,
    OPT_DBOP,
];

/// Parses `oracle.auth_mode`.
pub(crate) fn auth_mode(value: &str) -> Result<u8> {
    Ok(match value.trim().to_ascii_lowercase().as_str() {
        "default" | "" => oracledb::AUTH_MODE_DEFAULT,
        "sysdba" => oracledb::AUTH_MODE_SYSDBA,
        "sysoper" => oracledb::AUTH_MODE_SYSOPER,
        "sysasm" => oracledb::AUTH_MODE_SYSASM,
        "sysbackup" | "sysbkp" => oracledb::AUTH_MODE_SYSBKP,
        "sysdg" | "sysdgd" => oracledb::AUTH_MODE_SYSDGD,
        "syskm" | "syskmt" => oracledb::AUTH_MODE_SYSKMT,
        "sysrac" => oracledb::AUTH_MODE_SYSRAC,
        other => {
            return Err(error(
                format!(
                    "invalid {OPT_AUTH_MODE} {other:?}; expected default, sysdba, sysoper, \
                     sysasm, sysbackup, sysdg, syskm, or sysrac"
                ),
                Status::InvalidArguments,
            ))
        }
    })
}

/// Masks the password (and `wallet_password` parameter) of an `oracle://`
/// URI.
pub(crate) fn redact_uri(uri: &str) -> String {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return uri.to_string();
    };
    let (main, query) = match rest.split_once('?') {
        Some((m, q)) => (m, Some(q)),
        None => (rest, None),
    };
    let main = match main.rsplit_once('@') {
        Some((userinfo, host)) => match userinfo.split_once(':') {
            Some((user, _)) => format!("{user}:***@{host}"),
            None => format!("{userinfo}@{host}"),
        },
        None => main.to_string(),
    };
    let query = query.map(|q| {
        q.split('&')
            .map(|pair| match pair.split_once('=') {
                Some((k, _)) if k.eq_ignore_ascii_case("wallet_password") => format!("{k}=***"),
                _ => pair.to_string(),
            })
            .collect::<Vec<_>>()
            .join("&")
    });
    match query {
        Some(q) => format!("{scheme}://{main}?{q}"),
        None => format!("{scheme}://{main}"),
    }
}

/// Database-level options as provided through the ADBC option API.
#[derive(Clone, Default)]
pub(crate) struct DbOptions {
    values: HashMap<String, OptionValue>,
}

impl DbOptions {
    pub(crate) fn set(&mut self, key: &str, value: OptionValue) -> Result<()> {
        if !KNOWN_OPTIONS.contains(&key) {
            return Err(error(
                format!(
                    "unknown Orkidea database option {key:?}; supported options: {}",
                    KNOWN_OPTIONS.join(", ")
                ),
                Status::InvalidArguments,
            ));
        }
        // Validate eagerly so failures happen at set_option time, not at
        // connection time.
        match key {
            OPT_STMT_CACHE_SIZE
            | OPT_CONNECT_TIMEOUT_MS
            | OPT_CALL_TIMEOUT_MS
            | OPT_BATCH_SIZE
            | OPT_FETCH_ARRAY_SIZE
            | OPT_PREFETCH_ROWS => {
                let v = expect_int(&value, key)?;
                if v < 0 {
                    return Err(error(
                        format!("option {key:?} must be non-negative"),
                        Status::InvalidArguments,
                    ));
                }
            }
            OPT_UNCONSTRAINED_NUMBER => {
                expect_string(&value, key)?.parse::<UnconstrainedNumber>()?;
            }
            OPT_AUTH_MODE => {
                auth_mode(&expect_string(&value, key)?)?;
            }
            _ if SECRET_OPTIONS.contains(&key) => {
                if !matches!(value, OptionValue::String(_)) {
                    return Err(error(
                        format!("option {key:?} expects a string"),
                        Status::InvalidArguments,
                    ));
                }
            }
            OPT_UNCONSTRAINED_NUMBER_SCALE => {
                validate_number_scale(expect_int(&value, key)?)?;
            }
            OPT_COLUMNAR_FETCH => {
                expect_bool(&value, key)?;
            }
            _ => {
                expect_string(&value, key)?;
            }
        }
        self.values.insert(key.to_string(), value);
        Ok(())
    }

    /// Type-planning options configured at the database level.
    pub(crate) fn type_options(&self) -> Result<TypeOptions> {
        let mut options = TypeOptions::default();
        if let Some(mode) = self.get_string(OPT_UNCONSTRAINED_NUMBER)? {
            options.unconstrained_number = mode.parse()?;
        }
        if let Some(scale) = self.get_i64(OPT_UNCONSTRAINED_NUMBER_SCALE)? {
            options.unconstrained_number_scale = validate_number_scale(scale)?;
        }
        Ok(options)
    }

    /// Whether query results are fetched column by column (default true).
    pub(crate) fn columnar_fetch(&self) -> Result<bool> {
        match self.values.get(OPT_COLUMNAR_FETCH) {
            Some(v) => expect_bool(v, OPT_COLUMNAR_FETCH),
            None => Ok(true),
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<&OptionValue> {
        self.values.get(key)
    }

    pub(crate) fn get_string(&self, key: &str) -> Result<Option<String>> {
        match self.values.get(key) {
            Some(v) => Ok(Some(expect_string(v, key)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn get_i64(&self, key: &str) -> Result<Option<i64>> {
        match self.values.get(key) {
            Some(v) => Ok(Some(expect_int(v, key)?)),
            None => Ok(None),
        }
    }

    /// Builds an `oracledb` configuration from the ADBC options.
    ///
    /// Precedence: explicit `username`/`password`/`oracle.connect_string`
    /// options override values embedded in the `uri`.
    pub(crate) fn build_config(&self) -> Result<oracledb::Config> {
        let mut parsed = ParsedUri::default();

        if let Some(uri) = self.get_string(OPT_URI)? {
            if !uri.trim().is_empty() {
                parsed = parse_uri(&uri)?;
            }
        }

        let connect_string = self
            .get_string(OPT_CONNECT_STRING)?
            .or(parsed.connect_string)
            .ok_or_else(|| {
                error(
                    "missing Oracle connect string: set the ADBC `uri` option (for example \
                     oracle://user:pass@host:1521/FREEPDB1) or `oracle.connect_string`",
                    Status::InvalidArguments,
                )
            })?;

        let user = self
            .get_string(OPT_USERNAME)?
            .or(parsed.username)
            .ok_or_else(|| {
                error(
                    "missing Oracle username: set the ADBC `username` option or embed it in `uri`",
                    Status::InvalidArguments,
                )
            })?;
        let password = self
            .get_string(OPT_PASSWORD)?
            .or(parsed.password)
            .ok_or_else(|| {
                error(
                    "missing Oracle password: set the ADBC `password` option or embed it in `uri`",
                    Status::InvalidArguments,
                )
            })?;

        let config_dir = self.get_string(OPT_CONFIG_DIR)?.or(parsed.config_dir);
        let wallet_location = self
            .get_string(OPT_WALLET_LOCATION)?
            .or(parsed.wallet_location);
        let wallet_password = self
            .get_string(OPT_WALLET_PASSWORD)?
            .or(parsed.wallet_password);
        let stmt_cache_size = self
            .get_i64(OPT_STMT_CACHE_SIZE)?
            .or(parsed.stmt_cache_size);
        let connect_timeout_ms = self
            .get_i64(OPT_CONNECT_TIMEOUT_MS)?
            .or(parsed.connect_timeout_ms);

        let mut config = oracledb::Config::default();
        config = config.set_driver_name(format!(
            "{} : {}",
            crate::DRIVER_NAME,
            crate::DRIVER_VERSION
        ));
        config = config
            .set_program(crate::DRIVER_NAME)
            .map_err(from_oracle)?;
        if let Some(dir) = config_dir {
            config = config.set_config_dir(&dir);
        }
        if let Some(location) = wallet_location {
            config = config.set_wallet_location(location);
        }
        if let Some(password) = wallet_password {
            config = config.set_wallet_password(&password);
        }
        if let Some(size) = stmt_cache_size {
            config = config.set_stmtcachesize(size as usize);
        }
        if let Some(ms) = connect_timeout_ms {
            config = config.set_transport_connect_timeout(Some(Duration::from_millis(ms as u64)));
        }
        if let Some(program) = self.get_string(OPT_PROGRAM)? {
            config = config.set_program(program).map_err(from_oracle)?;
        }
        if let Some(machine) = self.get_string(OPT_MACHINE)? {
            config = config.set_machine(machine).map_err(from_oracle)?;
        }
        if let Some(terminal) = self.get_string(OPT_TERMINAL)? {
            config = config.set_terminal(terminal);
        }
        if let Some(cclass) = self.get_string(OPT_CCLASS)? {
            config = config.set_cclass(cclass);
        }

        if let Some(mode) = self.get_string(OPT_AUTH_MODE)? {
            config = config.set_auth_mode(auth_mode(&mode)?);
        }
        if let Some(new_password) = self.get_string(OPT_NEW_PASSWORD)? {
            config = config.set_new_password(&new_password);
        }

        config = config
            .set_connect_string(&connect_string)
            .map_err(from_oracle)?;
        config = config.set_credentials(&user, &password);
        Ok(config)
    }
}

#[derive(Default)]
struct ParsedUri {
    username: Option<String>,
    password: Option<String>,
    connect_string: Option<String>,
    config_dir: Option<String>,
    wallet_location: Option<String>,
    wallet_password: Option<String>,
    stmt_cache_size: Option<i64>,
    connect_timeout_ms: Option<i64>,
}

fn parse_uri(uri: &str) -> Result<ParsedUri> {
    let (scheme, rest) = match uri.split_once("://") {
        Some((scheme, rest)) => (scheme.to_ascii_lowercase(), rest),
        None => {
            // Treat a bare string as an Oracle connect string/alias.
            return Ok(ParsedUri {
                connect_string: Some(uri.to_string()),
                ..Default::default()
            });
        }
    };

    let tcps = match scheme.as_str() {
        "oracle" => false,
        "oracle+tcps" => true,
        other => {
            return Err(error(
                format!("unsupported Oracle URI scheme {other:?}; use oracle:// or oracle+tcps://"),
                Status::InvalidArguments,
            ));
        }
    };

    let (authority_and_path, query) = match rest.split_once('?') {
        Some((left, right)) => (left, Some(right)),
        None => (rest, None),
    };

    let (userinfo, host_and_path) = match authority_and_path.rsplit_once('@') {
        Some((userinfo, rest)) => (Some(userinfo), rest),
        None => (None, authority_and_path),
    };

    let (mut username, mut password) = (None, None);
    if let Some(userinfo) = userinfo {
        let (u, p) = match userinfo.split_once(':') {
            Some((u, p)) => (u, Some(p)),
            None => (userinfo, None),
        };
        username = Some(decode(u)?);
        password = p.map(decode).transpose()?;
    }

    let (host_port, service) = match host_and_path.split_once('/') {
        Some((hp, service)) => (hp, Some(service)),
        None => (host_and_path, None),
    };

    let service = service.filter(|s| !s.is_empty()).map(decode).transpose()?;

    let (host, port) = split_host_port(host_port)?;

    let mut parsed = ParsedUri {
        username,
        password,
        ..Default::default()
    };

    let mut protocol = if tcps { "TCPS" } else { "TCP" }.to_string();
    let mut sid = None;
    let mut service_name = service;

    if let Some(query) = query {
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').ok_or_else(|| {
                error(
                    format!("invalid Oracle URI query parameter {pair:?}"),
                    Status::InvalidArguments,
                )
            })?;
            let value = decode(value)?;
            match key.to_ascii_lowercase().as_str() {
                "service_name" | "service" => service_name = Some(value),
                "sid" => sid = Some(value),
                "protocol" => protocol = value.to_ascii_uppercase(),
                "config_dir" => parsed.config_dir = Some(value),
                "wallet_location" => parsed.wallet_location = Some(value),
                "wallet_password" => parsed.wallet_password = Some(value),
                "stmt_cache_size" => parsed.stmt_cache_size = Some(parse_i64(key, &value)?),
                "connect_timeout_ms" | "transport_connect_timeout_ms" => {
                    parsed.connect_timeout_ms = Some(parse_i64(key, &value)?)
                }
                other => {
                    return Err(error(
                        format!("unsupported Oracle URI query parameter {other:?}"),
                        Status::InvalidArguments,
                    ));
                }
            }
        }
    }

    if protocol != "TCP" && protocol != "TCPS" {
        return Err(error(
            format!("unsupported Oracle protocol {protocol:?}; use TCP or TCPS"),
            Status::InvalidArguments,
        ));
    }

    let connect_data = if let Some(sid) = sid {
        format!("(SID={})", quote_descriptor_value(&sid))
    } else if let Some(service) = service_name {
        format!("(SERVICE_NAME={})", quote_descriptor_value(&service))
    } else {
        return Err(error(
            "Oracle URI is missing a service name or sid",
            Status::InvalidArguments,
        ));
    };

    parsed.connect_string = Some(format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL={protocol})(HOST={})(PORT={}))(CONNECT_DATA={}))",
        quote_descriptor_value(&host),
        port,
        connect_data
    ));
    Ok(parsed)
}

fn split_host_port(host_port: &str) -> Result<(String, u16)> {
    if host_port.is_empty() {
        return Err(error(
            "Oracle URI is missing a host",
            Status::InvalidArguments,
        ));
    }
    if host_port.starts_with('[') {
        // IPv6 literal: [::1]:1521
        let end = host_port.find(']').ok_or_else(|| {
            error(
                "invalid IPv6 literal in Oracle URI",
                Status::InvalidArguments,
            )
        })?;
        let host = &host_port[1..end];
        let port = match host_port[end + 1..].strip_prefix(':') {
            Some(p) if !p.is_empty() => p
                .parse::<u16>()
                .map_err(|_| error("invalid port in Oracle URI", Status::InvalidArguments))?,
            _ => 1521,
        };
        return Ok((host.to_string(), port));
    }
    match host_port.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
            let port = port
                .parse::<u16>()
                .map_err(|_| error("invalid port in Oracle URI", Status::InvalidArguments))?;
            Ok((host.to_string(), port))
        }
        _ => Ok((host_port.to_string(), 1521)),
    }
}

/// Oracle descriptor values must not be able to break out of the descriptor.
fn quote_descriptor_value(value: &str) -> String {
    value
        .chars()
        .filter(|c| *c != '(' && *c != ')' && *c != '=' && *c != ' ')
        .collect()
}

fn parse_i64(key: &str, value: &str) -> Result<i64> {
    value.parse::<i64>().map_err(|_| {
        error(
            format!("URI query parameter {key:?} must be an integer, got {value:?}"),
            Status::InvalidArguments,
        )
    })
}

fn decode(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(error(
                        "invalid percent-encoding in Oracle URI",
                        Status::InvalidArguments,
                    ));
                }
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).map_err(|_| {
                    error(
                        "invalid percent-encoding in Oracle URI",
                        Status::InvalidArguments,
                    )
                })?;
                let byte = u8::from_str_radix(hex, 16).map_err(|_| {
                    error(
                        "invalid percent-encoding in Oracle URI",
                        Status::InvalidArguments,
                    )
                })?;
                out.push(byte);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| {
        error(
            "Oracle URI contains invalid UTF-8",
            Status::InvalidArguments,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_easy_uri() {
        let p = parse_uri("oracle://scott:tiger@db.example.com:1522/FREEPDB1").unwrap();
        assert_eq!(p.username.as_deref(), Some("scott"));
        assert_eq!(p.password.as_deref(), Some("tiger"));
        let cs = p.connect_string.unwrap();
        assert!(cs.contains("HOST=db.example.com"));
        assert!(cs.contains("PORT=1522"));
        assert!(cs.contains("SERVICE_NAME=FREEPDB1"));
        assert!(cs.contains("PROTOCOL=TCP"));
    }

    #[test]
    fn redacts_secrets_in_uris() {
        assert_eq!(
            redact_uri("oracle://scott:tiger@db:1521/svc?wallet_password=x&stmt_cache_size=5"),
            "oracle://scott:***@db:1521/svc?wallet_password=***&stmt_cache_size=5"
        );
        assert_eq!(redact_uri("oracle://db:1521/svc"), "oracle://db:1521/svc");
        assert_eq!(redact_uri("mydb_high"), "mydb_high");
    }

    #[test]
    fn auth_modes() {
        assert_eq!(auth_mode("SYSDBA").unwrap(), oracledb::AUTH_MODE_SYSDBA);
        assert_eq!(auth_mode("sysbackup").unwrap(), oracledb::AUTH_MODE_SYSBKP);
        assert!(auth_mode("root").is_err());
    }

    #[test]
    fn parses_tcps_wallet_uri() {
        let p = parse_uri(
            "oracle+tcps://u:p%40ss@[::1]:2484/svc?wallet_location=%2Ftmp%2Fwallet&stmt_cache_size=20",
        )
        .unwrap();
        assert_eq!(p.password.as_deref(), Some("p@ss"));
        assert_eq!(p.wallet_location.as_deref(), Some("/tmp/wallet"));
        assert_eq!(p.stmt_cache_size, Some(20));
        assert!(p.connect_string.unwrap().contains("PROTOCOL=TCPS"));
    }

    #[test]
    fn rejects_unknown_query_parameter() {
        assert!(parse_uri("oracle://u:p@h:1521/svc?evil=1").is_err());
    }

    #[test]
    fn rejects_unknown_option() {
        let mut o = DbOptions::default();
        assert!(o.set("oracle.nope", "x".into()).is_err());
    }
}
