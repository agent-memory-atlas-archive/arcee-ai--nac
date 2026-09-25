use std::time::Duration;

use serde_json::Value;

use super::ToolResult;

pub(crate) const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub(crate) const MAX_TOOL_TIMEOUT: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolTimeoutDisposition {
    Bounded,
    Settle,
    Delegated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToolTimeout {
    pub duration: Duration,
    pub disposition: ToolTimeoutDisposition,
    pub remote_outcome_uncertain: bool,
}

impl ToolTimeout {
    pub fn bounded(duration: Duration) -> Self {
        Self {
            duration,
            disposition: ToolTimeoutDisposition::Bounded,
            remote_outcome_uncertain: false,
        }
    }
}

/// Decorate an object schema with NAC's reserved execution envelope.
pub(crate) fn decorate_timeout_schema(parameters: &mut Value) -> Result<(), &'static str> {
    let object = parameters
        .as_object_mut()
        .ok_or("input schema is not an object")?;
    if object.get("type").and_then(Value::as_str) != Some("object") {
        return Err("input schema root must declare type 'object'");
    }
    let properties = object
        .entry("properties")
        .or_insert_with(|| Value::Object(serde_json::Map::new()))
        .as_object_mut()
        .ok_or("input schema properties are not an object")?;
    let envelope = serde_json::json!({
        "type": ["object", "null"],
        "additionalProperties": false,
        "properties": {
            "timeout_ms": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_TOOL_TIMEOUT.as_millis() as u64
            }
        },
        "required": ["timeout_ms"]
    });
    if let Some(existing) = properties.get("_nac") {
        if existing != &envelope {
            return Err("input schema already defines reserved property '_nac'");
        }
    } else {
        properties.insert("_nac".to_string(), envelope);
    }

    // OpenAI strict-mode schemas mark every property as required and encode
    // optionality through nullable types. Preserve that invariant when the
    // source schema was strict-compatible; ordinary schemas keep `_nac`
    // genuinely optional.
    let property_count = properties.len();
    if let Some(required) = object.get_mut("required").and_then(Value::as_array_mut) {
        let already_required = required.iter().any(|value| value.as_str() == Some("_nac"));
        if !already_required && required.len() + 1 == property_count {
            required.push(Value::String("_nac".to_string()));
        }
    }
    Ok(())
}

pub(super) fn strip_timeout_envelope(input: &mut Value) -> Result<Option<Duration>, ToolResult> {
    let Some(object) = input.as_object_mut() else {
        return Ok(None);
    };
    let Some(envelope) = object.remove("_nac") else {
        return Ok(None);
    };
    if envelope.is_null() {
        return Ok(None);
    }
    let Some(envelope) = envelope.as_object() else {
        return Err(ToolResult::text(
            "Error: '_nac' must be an object containing only 'timeout_ms'",
            true,
        ));
    };
    if envelope.keys().any(|key| key != "timeout_ms") {
        return Err(ToolResult::text(
            "Error: '_nac' contains an unsupported field; only 'timeout_ms' is accepted",
            true,
        ));
    }
    let Some(value) = envelope.get("timeout_ms") else {
        return Err(ToolResult::text(
            "Error: '_nac.timeout_ms' is required when '_nac' is an object",
            true,
        ));
    };
    let Some(milliseconds) = value.as_u64() else {
        return Err(ToolResult::text(
            "Error: '_nac.timeout_ms' must be an integer",
            true,
        ));
    };
    if milliseconds == 0 || milliseconds > MAX_TOOL_TIMEOUT.as_millis() as u64 {
        return Err(ToolResult::text(
            format!(
                "Error: '_nac.timeout_ms' must be between 1 and {}",
                MAX_TOOL_TIMEOUT.as_millis()
            ),
            true,
        ));
    }
    Ok(Some(Duration::from_millis(milliseconds)))
}
