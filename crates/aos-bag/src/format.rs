use crate::{BagError, BagResult, ChannelInfo, ChannelRole, FORMAT_VERSION, PROFILE};
use aos_core::Interface;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) fn channel_metadata(channel: &ChannelInfo) -> BagResult<BTreeMap<String, String>> {
    Ok(BTreeMap::from([
        ("aos.format_version".into(), FORMAT_VERSION.to_string()),
        ("aos.channel_id".into(), channel.id.clone()),
        ("aos.type_name".into(), channel.type_name.clone()),
        ("aos.type_hash".into(), channel.type_hash.clone()),
        (
            "aos.interface".into(),
            serde_json::to_string(&channel.interface)?,
        ),
        ("aos.schema_part".into(), "message".into()),
        (
            "aos.offered_qos".into(),
            serde_json::to_string(&channel.offered_qos)?,
        ),
        (
            "aos.record_qos".into(),
            serde_json::to_string(&channel.record_qos)?,
        ),
        (
            "aos.log_time_domain".into(),
            channel.log_time_domain.clone(),
        ),
        (
            "aos.publish_time_source".into(),
            channel.publish_time_source.clone(),
        ),
        (
            "aos.sequence_epoch".into(),
            channel.sequence_epoch.to_string(),
        ),
        (
            "aos.channel_role".into(),
            match channel.role {
                ChannelRole::Business => "business",
                ChannelRole::SampleMetadata => "sample_metadata",
            }
            .into(),
        ),
        (
            "aos.companion_for".into(),
            serde_json::to_string(&channel.companion_for)?,
        ),
        (
            "aos.action_observation".into(),
            serde_json::to_string(&channel.action)?,
        ),
    ]))
}

pub(crate) fn decode_channel(
    channel: &mcap::records::Channel,
    schema_name: &str,
    schema_encoding: &str,
    schema_data: &[u8],
) -> BagResult<(ChannelInfo, Interface)> {
    let fields = &channel.metadata;
    let field = |name: &str| {
        fields
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| BagError::corrupt(format!("missing channel field {name}")))
    };
    if channel.message_encoding != "json"
        || schema_encoding != "jsonschema"
        || field("aos.format_version")? != "1"
        || field("aos.schema_part")? != "message"
    {
        return Err(BagError::corrupt(
            "unsupported AOS channel/schema encoding or version",
        ));
    }
    let info = ChannelInfo {
        id: field("aos.channel_id")?.into(),
        topic: channel.topic.clone(),
        type_name: field("aos.type_name")?.into(),
        type_hash: field("aos.type_hash")?.into(),
        interface: serde_json::from_str(field("aos.interface")?)?,
        offered_qos: serde_json::from_str(field("aos.offered_qos")?)?,
        record_qos: serde_json::from_str(field("aos.record_qos")?)?,
        log_time_domain: field("aos.log_time_domain")?.into(),
        publish_time_source: field("aos.publish_time_source")?.into(),
        sequence_epoch: field("aos.sequence_epoch")?
            .parse()
            .map_err(|_| BagError::corrupt("invalid sequence epoch"))?,
        action: serde_json::from_str(field("aos.action_observation")?)?,
        role: match field("aos.channel_role")? {
            "business" => ChannelRole::Business,
            "sample_metadata" => ChannelRole::SampleMetadata,
            _ => return Err(BagError::corrupt("unknown AOS channel role")),
        },
        companion_for: serde_json::from_str(field("aos.companion_for")?)?,
    };
    let interface = info.validate()?;
    let schema: Value = serde_json::from_slice(schema_data)?;
    if schema_name != info.type_name || schema != info.interface["message"] {
        return Err(BagError::corrupt(
            "MCAP schema and complete AOS Interface disagree",
        ));
    }
    Ok((info, interface))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordingHeader {
    pub profile: String,
    pub format_version: u32,
    pub recording_id: String,
    pub system_anchor_ns: u64,
    pub writer_version: String,
    pub configuration: Value,
}
impl RecordingHeader {
    pub(crate) fn validate(&self) -> BagResult<()> {
        if self.profile != PROFILE
            || self.format_version != FORMAT_VERSION
            || uuid::Uuid::parse_str(&self.recording_id).is_err()
        {
            return Err(BagError::corrupt("invalid AOS recording header"));
        }
        Ok(())
    }
}
