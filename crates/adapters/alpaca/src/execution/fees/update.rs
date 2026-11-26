// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{any::Any, sync::Arc};

use nautilus_core::UnixNanos;
use nautilus_model::data::{CustomDataTrait, HasTsInit};

use super::AlpacaFeeSyncUpdate;

// The batch represents a poll through one timestamp. Individual fee rows carry
// their broker creation times, including fees posted before an overlapping poll.
impl HasTsInit for AlpacaFeeSyncUpdate {
    fn ts_init(&self) -> UnixNanos {
        self.through
    }
}

impl CustomDataTrait for AlpacaFeeSyncUpdate {
    fn type_name(&self) -> &'static str {
        Self::type_name_static()
    }

    fn type_name_static() -> &'static str {
        stringify!(AlpacaFeeSyncUpdate)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.through
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        Ok(Arc::new(serde_json::from_value::<Self>(value)?))
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
}
