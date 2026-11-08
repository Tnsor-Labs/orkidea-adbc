//! C ABI entry point for the Oracle Arrow ADBC driver.

adbc_ffi::export_driver!(OrkideaDriverInit, oracle_adbc::OracleDriver);
