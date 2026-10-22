//! JSON stdout is carried without parsing numbers into floating-point values.
use std::collections::BTreeSet;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

struct UniqueObject(Vec<Box<RawValue>>);

impl<'de> Deserialize<'de> for UniqueObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = UniqueObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an object without duplicate keys")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut keys = BTreeSet::new();
                let mut values = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key) {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                    values.push(map.next_value()?);
                }
                Ok(UniqueObject(values))
            }
        }
        deserializer.deserialize_map(ObjectVisitor)
    }
}

fn unique_keys(raw: &RawValue, depth: usize) -> Option<()> {
    if depth > 128 {
        return None;
    }
    let children = match raw.get().as_bytes().first()? {
        b'{' => serde_json::from_str::<UniqueObject>(raw.get()).ok()?.0,
        b'[' => serde_json::from_str::<Vec<Box<RawValue>>>(raw.get()).ok()?,
        _ => return Some(()),
    };
    for child in children {
        unique_keys(&child, depth + 1)?;
    }
    Some(())
}

pub(crate) fn stdout_details(bytes: &[u8]) -> Option<Box<RawValue>> {
    let raw: Box<RawValue> = serde_json::from_slice(bytes).ok()?;
    unique_keys(&raw, 0)?;
    RawValue::from_string(format!("{{\"value\":{}}}", raw.get())).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_json_is_retained_without_precision_or_range_conversion() {
        let input = br#"{"id":12345678901234567890123,"value":1.0000000000000000001,"huge":1e400}"#;
        let details = stdout_details(input).unwrap();
        assert_eq!(
            details.get(),
            format!("{{\"value\":{}}}", std::str::from_utf8(input).unwrap())
        );
        assert_eq!(serde_json::to_string(&details).unwrap(), details.get());
    }

    #[test]
    fn duplicate_keys_and_invalid_json_are_diagnostics_not_business_values() {
        for input in [
            r#"{"a":1,"a":2}"#,
            r#"{"nested":[{"a":1,"\u0061":2}]}"#,
            r#"{"a":1} {"b":2}"#,
            r#"{"a":NaN}"#,
        ] {
            assert!(stdout_details(input.as_bytes()).is_none(), "{input}");
        }
        assert!(stdout_details(br#"{"a":[1,{"b":true}],"c":null}"#).is_some());
    }
}
