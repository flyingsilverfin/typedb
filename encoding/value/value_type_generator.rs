/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::sync::atomic::AtomicU16;
use storage::snapshot::WritableSnapshot;
use crate::error::EncodingError;
use crate::graph::type_::Kind::{Attribute, Entity, Relation, Role};
use crate::graph::type_::vertex::{build_vertex_attribute_type, build_vertex_entity_type, build_vertex_relation_type, build_vertex_role_type, TypeVertex};
use crate::graph::type_::vertex_generator::TypeVertexAllocator;
use crate::value::value_type::ValueType;

pub struct ValueTypeGenerator {
    next_value_type: AtomicU16,
}

impl Default for ValueTypeGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueTypeGenerator {
    const U16_LENGTH: usize = std::mem::size_of::<u16>();

    pub fn new() -> ValueTypeGenerator {
       ValueTypeGenerator {
           next_value_type: AtomicU16::new(0),
        }
    }

    pub fn create_value_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
    ) -> Result<ValueType<'static>, EncodingError> {
        let vertex = self.next_attribute.allocate(snapshot)?;
        snapshot.put(vertex.as_storage_key().into_owned_array());
        Ok(vertex)
    }
}
