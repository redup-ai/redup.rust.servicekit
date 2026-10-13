//! gRPC server reflection integration.
//!
//! Analog of `redup_servicekit.grpc.server` reflection setup in Python:
//! the service provides the encoded descriptor set and advertised service
//! names, this module builds the tonic v1 reflection service.
//!
//! The descriptor set is produced at build time:
//!
//! ```ignore
//! // build.rs
//! let out_dir = std::env::var("OUT_DIR")?;
//! tonic_build::configure()
//!     .file_descriptor_set_path(std::path::PathBuf::from(out_dir).join("descriptor.bin"))
//!     .compile_protos(&["proto/service.proto"], &["proto"])?;
//! ```
//!
//! and included with [`tonic::include_file_descriptor_set!`].

/// Build the v1 server reflection service for the given encoded
/// `FileDescriptorSet` and advertised fully-qualified service names.
pub fn server_v1(
    file_descriptor_set: &[u8],
    service_names: &[&str],
) -> Result<
    tonic_reflection::server::v1::ServerReflectionServer<
        impl tonic_reflection::server::v1::ServerReflection,
    >,
    tonic_reflection::server::Error,
> {
    let mut builder = tonic_reflection::server::Builder::configure();
    for name in service_names {
        builder = builder.with_service_name(*name);
    }
    builder
        .register_encoded_file_descriptor_set(file_descriptor_set)
        .build_v1()
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn descriptor_set() -> Vec<u8> {
        use prost_types::{FileDescriptorProto, FileDescriptorSet, ServiceDescriptorProto};
        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("example.proto".to_string()),
                package: Some("example.v1".to_string()),
                service: vec![ServiceDescriptorProto {
                    name: Some("ExampleService".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        fds.encode_to_vec()
    }

    #[test]
    fn builds_v1_server_from_descriptor_set() {
        let encoded = descriptor_set();
        let server = server_v1(&encoded, &["example.v1.ExampleService"]);
        assert!(
            server.is_ok(),
            "valid descriptor set must be accepted: {:?}",
            server.err()
        );
    }

    #[test]
    fn rejects_broken_descriptor_set() {
        assert!(server_v1(b"not a descriptor set", &[]).is_err());
    }
}
