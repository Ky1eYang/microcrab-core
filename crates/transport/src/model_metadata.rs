//! Public market metadata, independent of inference transports and credentials.
//! The embedding server owns persistence and refresh scheduling.
mod manager;
mod types;

pub use manager::{ModelMetadataManager, OPENROUTER_MODELS_ENDPOINT, OpenRouterModelManager};
pub use types::{
    MetadataError, MetadataSnapshot, ModelArchitecture, ModelDefaultParameters, ModelMatch,
    ModelMatchKind, ModelMetadata, ModelPerRequestLimits, ModelPricing, ModelTopProvider,
};

#[cfg(test)]
mod tests;
