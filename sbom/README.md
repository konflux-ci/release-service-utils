# Konflux release-time SBOM generation
This module contains scripts and libraries used to enrich SBOMs created at
build-time (component-level) and create SBOMs (product-level) when a snapshot is
being released.

## Data sources
### Snapshot spec
The necessary data source for both component-level and product-level SBOM
manipulation is the mapped snapshot spec in JSON format. This file is created by
the `apply-mapping` Tekton Task, which exports a result containing the path to
the file.

The snapshot spec is parsed using the `sbom.sbomlib.make_snapshot` function into
a Python representation of the snapshot. The `make_snapshot` function also
handles multiarch images by fetching manifests of index images (using `oras`)
and parsing them. The resulting object contains all component repositories and
digests of images that are being released in the snapshot.
     
### Data file
The product-level SBOM requires additional data to be constructed (such as the
cpe id, product name and version). This data can be found in the data file,
which is exported by the `collect-data` Tekton task as path to a file.

## Component-level SBOM enrichment
When component-level SBOMs are updated, the OCI PURLs generated during
build-time are stripped and new PURLs are generated based on the parsed snapshot
data. Non-OCI PURLs are preserved.

## Product-level SBOM creation
This should be filled once product-level SBOMs use the new inputs.
