/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

pub type ValueTypeIDUInt = u16;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ValueTypeID {
    bytes: [u8; ValueTypeID::LENGTH],
}

impl ValueTypeID {
    pub(crate) const LENGTH: usize = std::mem::size_of::<ValueTypeIDUInt>();

    pub const fn new(bytes: [u8; ValueTypeID::LENGTH]) -> Self {
        ValueTypeID { bytes }
    }

    pub fn build(id: ValueTypeIDUInt) -> Self {
        debug_assert_eq!(std::mem::size_of_val(&id), ValueTypeID::LENGTH);
        Self { bytes: id.to_be_bytes() }
    }

    pub fn bytes(&self) -> [u8; ValueTypeID::LENGTH] {
        self.bytes
    }
}

// TODO: how do we handle user-created compound structs?
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ValueType {
    Boolean,
    Long,
    Double,

    // TODO: consider splitting with/without timezone
    DateTime,
    /*
    Duration, // Naming: 'interval'?
     */
    String,

    Struct { id: ValueTypeID, definition: StructDefinition },
}

macro_rules! value_type_functions {
    ($(
        $name:ident => $bytes:tt
    ),+ $(,)?) => {
        pub fn value_type_id(&self) -> ValueTypeID {
            let bytes = match self {
                $(
                    Self::$name => &$bytes,
                )+
            };
            ValueTypeID::new(*bytes)
        }

        pub fn from_value_type_id(value_type_id: ValueTypeID) -> Self {
            match value_type_id.bytes() {
                $(
                    $bytes => Self::$name,
                )+
                _ => unreachable!(),
            }
       }
   };
}

impl ValueType {
    // value_type_functions!(
    //     Boolean => [0],
    //     Long => [1],
    //     Double => [2],
    //     String => [3],
    //     DateTime => [4],
    // );
}

/// Restrictions: maximum number fields is StructFieldNumber::MAX
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StructDefinition {
    name: String,
    fields: Vec<StructFieldDefinition>,
    field_names: HashMap<String, StructFieldNumber>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StructFieldDefinition {
    optional: bool,
    value_type: ValueType,
}

pub(crate) type StructFieldNumber = u16;
