use crate::master::meta::feature::AclFeature;
use curvine_common::proto::DirFeatureProto;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirFeature {
    pub(crate) x_attr: HashMap<String, Vec<u8>>,
    pub(crate) acl: AclFeature,
}

impl DirFeature {
    pub fn new() -> Self {
        Self {
            x_attr: HashMap::new(),
            acl: AclFeature::default(),
        }
    }
    pub fn dir_feature_to_pb(features: DirFeature) -> DirFeatureProto {
        DirFeatureProto {
            acl: AclFeature::acl_feature_to_pb(features.acl),
            x_attr: features.x_attr,
        }
    }

    pub fn dir_feature_from_pb(proto: DirFeatureProto) -> DirFeature {
        DirFeature {
            acl: AclFeature::acl_feature_from_pb(proto.acl),
            x_attr: proto.x_attr,
        }
    }
}

impl Default for DirFeature {
    fn default() -> Self {
        Self::new()
    }
}
