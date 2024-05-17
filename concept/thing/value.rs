/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;

use chrono::NaiveDateTime;

use encoding::value::{
    boolean_bytes::BooleanBytes, date_time_bytes::DateTimeBytes, double_bytes::DoubleBytes, long_bytes::LongBytes,
    string_bytes::StringBytes, ValueEncodable,
};
use encoding::value::struct_bytes::{StructBytes, StructValue};
use encoding::value::value_type::StructDefinition;

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Boolean(bool),
    Long(i64),
    Double(f64),
    DateTime(NaiveDateTime),
    String(Cow<'a, str>),
    Struct(Cow<'a, StructValue>),
}

impl<'a> Value<'a> {
    pub fn as_reference(&self) -> Value<'_> {
        match self {
            Value::Boolean(boolean) => Value::Boolean(*boolean),
            Value::Long(long) => Value::Long(*long),
            Value::Double(double) => Value::Double(*double),
            Value::DateTime(date_time) => Value::DateTime(*date_time),
            Value::String(string) => Value::String(Cow::Borrowed(string.as_ref())),
            Value::Struct(map) => Value::Struct(Cow::Borrowed(map.as_ref())),
        }
    }

    pub fn unwrap_boolean(self) -> bool {
        match self {
            Self::Boolean(boolean) => boolean,
            _ => panic!("Cannot unwrap Long if not a long value."),
        }
    }

    pub fn unwrap_long(self) -> i64 {
        match self {
            Self::Long(long) => long,
            _ => panic!("Cannot unwrap Long if not a long value."),
        }
    }

    pub fn unwrap_double(self) -> f64 {
        match self {
            Self::Double(double) => double,
            _ => panic!("Cannot unwrap Double if not a double value."),
        }
    }

    pub fn unwrap_date_time(self) -> NaiveDateTime {
        match self {
            Self::DateTime(date_time) => date_time,
            _ => panic!("Cannot unwrap DateTime if not a datetime value."),
        }
    }

    pub fn unwrap_string(self) -> Cow<'a, str> {
        match self {
            Self::String(string) => string,
            _ => panic!("Cannot unwrap String if not a string value."),
        }
    }
}

impl<'a> ValueEncodable for Value<'a> {
    fn encode_boolean(&self) -> BooleanBytes {
        match self {
            Self::Boolean(boolean) => BooleanBytes::build(*boolean),
            _ => panic!("Cannot encoded non-boolean as BooleanBytes"),
        }
    }

    fn encode_long(&self) -> LongBytes {
        match self {
            Self::Long(long) => LongBytes::build(*long),
            _ => panic!("Cannot encoded non-long as LongBytes"),
        }
    }

    fn encode_double(&self) -> DoubleBytes {
        match self {
            Self::Double(double) => DoubleBytes::build(*double),
            _ => panic!("Cannot encoded non-double as DoubleBytes"),
        }
    }

    fn encode_date_time(&self) -> DateTimeBytes {
        match self {
            Self::DateTime(date_time) => DateTimeBytes::build(*date_time),
            _ => panic!("Cannot encoded non-datetime as DateTimeBytes"),
        }
    }

    fn encode_string(&self) -> StringBytes<'_, 64> {
        match self {
            Value::String(str) => StringBytes::build_ref(str),
            _ => panic!("Cannot encoded non-String as StringBytes"),
        }
    }

    fn encode_struct(&self, definition: &StructDefinition) -> StructBytes<64> {
        match self {
            Value::Struct(struct_) => StructBytes::build(struct_.as_ref(), &definition),
            _ => panic!("Cannot encoded non-Struct as StructBytes"),
        }
    }
}
