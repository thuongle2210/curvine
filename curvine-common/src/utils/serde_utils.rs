// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use orpc::{err_box, try_err, CommonResult};
use prost::Message;

pub struct SerdeUtils;

impl SerdeUtils {
    // The default serialization and deserialization functions. bincode is used here.
    pub fn serialize<T>(value: &T) -> CommonResult<Vec<u8>>
    where
        T: serde::Serialize,
    {
        let bytes = try_err!(bincode::serialize(value));
        Ok(bytes)
    }

    pub fn serialize_into<W, T>(writer: W, value: &T) -> CommonResult<()>
    where
        W: std::io::Write,
        T: serde::Serialize + ?Sized,
    {
        try_err!(bincode::serialize_into(writer, value));
        Ok(())
    }

    pub fn serialize_json<T>(value: &T) -> CommonResult<Vec<u8>>
    where
        T: serde::Serialize,
    {
        let bytes = try_err!(serde_json::to_vec(value));
        Ok(bytes)
    }

    pub fn serialize_json_into<W, T>(writer: W, value: &T) -> CommonResult<()>
    where
        W: std::io::Write,
        T: serde::Serialize + ?Sized,
    {
        try_err!(serde_json::to_writer(writer, value));
        Ok(())
    }

    pub fn deserialize<'a, T>(bytes: &'a [u8]) -> CommonResult<T>
    where
        T: serde::de::Deserialize<'a>,
    {
        let res = try_err!(bincode::deserialize::<T>(bytes));
        Ok(res)
    }

    pub fn deserialize_from<R, T>(reader: R) -> CommonResult<T>
    where
        R: std::io::Read,
        T: serde::de::DeserializeOwned,
    {
        let res = try_err!(bincode::deserialize_from::<R, T>(reader));
        Ok(res)
    }

    pub fn deserialize_json<'a, T>(bytes: &'a [u8]) -> CommonResult<T>
    where
        T: serde::de::Deserialize<'a>,
    {
        let res = try_err!(serde_json::from_slice::<T>(bytes));
        Ok(res)
    }

    pub fn deserialize_json_from<R, T>(reader: R) -> CommonResult<T>
    where
        R: std::io::Read,
        T: serde::de::DeserializeOwned,
    {
        let res = try_err!(serde_json::from_reader::<R, T>(reader));
        Ok(res)
    }
}

pub trait Serializer: Send + Sync {
    fn serialize<T: serde::Serialize>(&self, value: &T) -> CommonResult<Vec<u8>>;
    fn deserialize<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> CommonResult<T>;

    fn serialize_into<W: std::io::Write, T: serde::Serialize>(
        &self,
        writer: W,
        value: &T,
    ) -> CommonResult<()>;

    fn deserialize_from<R: std::io::Read, T: serde::de::DeserializeOwned>(
        &self,
        reader: R,
    ) -> CommonResult<T>;

    fn name(&self) -> &'static str;
}

/// Bincode serializer implementation  
pub struct BincodeSerializer;

impl Serializer for BincodeSerializer {
    fn serialize<T: serde::Serialize>(&self, value: &T) -> CommonResult<Vec<u8>> {
        let bytes = try_err!(bincode::serialize(value));
        Ok(bytes)
    }

    fn deserialize<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> CommonResult<T> {
        let res = try_err!(bincode::deserialize::<T>(bytes));
        Ok(res)
    }

    fn serialize_into<W: std::io::Write, T: serde::Serialize>(
        &self,
        writer: W,
        value: &T,
    ) -> CommonResult<()> {
        try_err!(bincode::serialize_into(writer, value));
        Ok(())
    }

    fn deserialize_from<R: std::io::Read, T: serde::de::DeserializeOwned>(
        &self,
        reader: R,
    ) -> CommonResult<T> {
        let res = try_err!(bincode::deserialize_from::<R, T>(reader));
        Ok(res)
    }

    fn name(&self) -> &'static str {
        "bincode"
    }
}

/// JSON serializer implementation  
pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize<T: serde::Serialize>(&self, value: &T) -> CommonResult<Vec<u8>> {
        let bytes = try_err!(serde_json::to_vec(value));
        Ok(bytes)
    }

    fn deserialize<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> CommonResult<T> {
        let res = try_err!(serde_json::from_slice::<T>(bytes));
        Ok(res)
    }

    fn serialize_into<W: std::io::Write, T: serde::Serialize>(
        &self,
        writer: W,
        value: &T,
    ) -> CommonResult<()> {
        try_err!(serde_json::to_writer(writer, value));
        Ok(())
    }

    fn deserialize_from<R: std::io::Read, T: serde::de::DeserializeOwned>(
        &self,
        reader: R,
    ) -> CommonResult<T> {
        let res = try_err!(serde_json::from_reader::<R, T>(reader));
        Ok(res)
    }

    fn name(&self) -> &'static str {
        "json"
    }
}

/// Protobuf serializer implementation  
pub struct ProtobufSerializer;

impl ProtobufSerializer {
    const VERSION_PREFIX: u8 = 0x01;

    pub fn is_valid_protobuf_version(bytes: &[u8]) -> bool {
        bytes[0] == Self::VERSION_PREFIX && !bytes.is_empty()
    }

    /// Serialize a protobuf Message type directly (most efficient)  
    pub fn serialize_message<T: Message>(&self, value: &T) -> CommonResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(value.encoded_len() + 1);
        buf.push(Self::VERSION_PREFIX);
        try_err!(value.encode(&mut buf));
        Ok(buf)
    }

    /// Deserialize a protobuf Message type directly (most efficient)  
    pub fn deserialize_message<T: Message + Default>(&self, bytes: &[u8]) -> CommonResult<T> {
        if bytes.is_empty() {
            return err_box!("Empty bytes for protobuf deserialization");
        }

        if !Self::is_valid_protobuf_version(bytes) {
            return err_box!(
                "Invalid protobuf version prefix: expected {}, got {}",
                Self::VERSION_PREFIX,
                bytes[0]
            );
        }

        let msg = try_err!(T::decode(&bytes[1..]));
        Ok(msg)
    }
}

impl Serializer for ProtobufSerializer {
    fn serialize<T: serde::Serialize>(&self, value: &T) -> CommonResult<Vec<u8>> {
        // Use JSON as intermediate format for serde types
        // Since all protobuf types have serde derives, this works
        let json_bytes = try_err!(serde_json::to_vec(value));
        let mut buf = Vec::with_capacity(json_bytes.len() + 1);
        buf.push(Self::VERSION_PREFIX);
        buf.extend_from_slice(&json_bytes);
        Ok(buf)
    }

    fn deserialize<'a, T: serde::de::Deserialize<'a>>(&self, bytes: &'a [u8]) -> CommonResult<T> {
        if bytes.is_empty() || bytes[0] != Self::VERSION_PREFIX {
            return err_box!("Invalid protobuf format");
        }
        let res = try_err!(serde_json::from_slice::<T>(&bytes[1..]));
        Ok(res)
    }

    fn serialize_into<W: std::io::Write, T: serde::Serialize>(
        &self,
        mut writer: W,
        value: &T,
    ) -> CommonResult<()> {
        let bytes = self.serialize(value)?;
        try_err!(writer.write_all(&bytes));
        Ok(())
    }

    fn deserialize_from<R: std::io::Read, T: serde::de::DeserializeOwned>(
        &self,
        mut reader: R,
    ) -> CommonResult<T> {
        let mut bytes = Vec::new();
        try_err!(reader.read_to_end(&mut bytes));
        self.deserialize(&bytes)
    }

    fn name(&self) -> &'static str {
        "protobuf"
    }
}

/// Serializer implementation enum for static dispatch (zero-cost abstraction)  
pub enum SerializerImpl {
    Bincode(BincodeSerializer),
    Protobuf(ProtobufSerializer),
    Json(JsonSerializer),
}

impl SerializerImpl {
    pub fn serialize<T: serde::Serialize>(&self, value: &T) -> CommonResult<Vec<u8>> {
        match self {
            Self::Bincode(s) => s.serialize(value),
            Self::Protobuf(s) => s.serialize(value),
            Self::Json(s) => s.serialize(value),
        }
    }

    pub fn deserialize<'a, T: serde::de::Deserialize<'a>>(
        &self,
        bytes: &'a [u8],
    ) -> CommonResult<T> {
        match self {
            Self::Bincode(s) => s.deserialize(bytes),
            Self::Protobuf(s) => s.deserialize(bytes),
            Self::Json(s) => s.deserialize(bytes),
        }
    }

    pub fn serialize_into<W: std::io::Write, T: serde::Serialize>(
        &self,
        writer: W,
        value: &T,
    ) -> CommonResult<()> {
        match self {
            Self::Bincode(s) => s.serialize_into(writer, value),
            Self::Protobuf(s) => s.serialize_into(writer, value),
            Self::Json(s) => s.serialize_into(writer, value),
        }
    }

    pub fn deserialize_from<R: std::io::Read, T: serde::de::DeserializeOwned>(
        &self,
        reader: R,
    ) -> CommonResult<T> {
        match self {
            Self::Bincode(s) => s.deserialize_from(reader),
            Self::Protobuf(s) => s.deserialize_from(reader),
            Self::Json(s) => s.deserialize_from(reader),
        }
    }
}
