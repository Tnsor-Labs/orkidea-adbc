//! C ABI entry point for the Orkidea Oracle ADBC driver.

adbc_ffi::export_driver!(OrkideaDriverInit, oracle_adbc::OracleDriver);
