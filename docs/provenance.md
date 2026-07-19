# Release provenance

Each stable tag is intended to identify one Python package version, one source commit, and one OCI image build.

## Python artifacts

The release workflow builds a wheel and source archive from the exact tag, validates metadata with Twine, installs and inspects the wheel, records SHA-256 checksums, and attaches the artifacts to the GitHub Release. Public-repository runs also create GitHub artifact attestations.

Verify downloaded files with the release's `SHA256SUMS` file before installation. The wheel exposes:

- `mad-mcp-ffmpeg`
- `mad-mcp-ffmpeg-worker`

The package does not bundle the FFmpeg binary.

## Container artifacts

Application images are published for `linux/amd64` to `ghcr.io/madpanda3d/ffmpeg-mcp-server`. Select the image by the `sha256` digest shown by the registry or release output, not by `latest` or a version tag. The immutable Compose manifests require only that digest for image selection.

OCI labels and baked runtime environment record the Git commit, semantic version, source URL, license, and source fingerprint. Release Compose does not override the baked commit or source fingerprint. The source fingerprint is computed from:

```bash
git archive --format=tar HEAD | sha256sum
```

The archive fingerprint complements the Git commit; it does not replace Git signature or artifact-attestation verification.

## Runtime readback

`GET /health` reports the package version, tool catalog version, exact tool count, baked build commit, baked source fingerprint, and deployment-declared image reference. Compare the baked fields with the OCI labels and release metadata. The image reference is not self-attestation: prove it externally by starting the exact digest and injecting that same digest through the immutable Compose manifest. A mutable tag or `development` value is not acceptable release provenance.

## FFmpeg provenance and license

The image installs Debian's FFmpeg package on a digest-pinned Debian base. The final application image digest is the immutable record of the exact installed system packages. Inspect a candidate image before deployment:

```bash
docker run --rm --entrypoint ffmpeg IMAGE_BY_DIGEST -version
docker run --rm --entrypoint ffmpeg IMAGE_BY_DIGEST -L
```

FFmpeg's applicable LGPL/GPL terms depend on the build and enabled components. The output of `ffmpeg -L` is authoritative for that image. The final image includes this project's `LICENSE` and `NOTICE` under `/usr/share/licenses/mad-mcp-ffmpeg/`. See [NOTICE](../NOTICE).
