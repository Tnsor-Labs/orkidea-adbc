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

use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Driver;

use crate::database::OracleDatabase;

/// ADBC driver entry point.
#[derive(Default)]
pub struct OracleDriver;

impl Driver for OracleDriver {
    type DatabaseType = OracleDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(OracleDatabase::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut db = OracleDatabase::default();
        for (key, value) in opts {
            use adbc_core::Optionable;
            db.set_option(key, value)?;
        }
        Ok(db)
    }
}
