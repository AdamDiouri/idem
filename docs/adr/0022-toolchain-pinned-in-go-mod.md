# D22. The Go toolchain is pinned in `go.mod`, and kept on a supported release

**Status:** Accepted 2026-11-10 · built in PR #12

## Context
The project built with whatever Go was installed locally (1.25.6, with `GOTOOLCHAIN=local`). When CI began running govulncheck, it found 28 standard-library vulnerabilities reachable from our code. Go 1.25 had left upstream support when 1.27 shipped, so some of them had no fix on 1.25 at all, and the Docker image shipped them too.

## Decision
`go.mod` pins the exact toolchain (`toolchain go1.27.4`). The go command downloads it when it is missing, so local builds, CI (`go-version-file: go.mod`) and the Docker image use the same version. govulncheck runs on every PR and weekly, so a newly published vulnerability fails CI until the toolchain or dependency is bumped.

## Consequences
- One version everywhere; "works on my machine" differences in the standard library disappear.
- Upgrades are deliberate commits that CI tests in full.
- We must keep to a supported Go release (the two most recent); the weekly scan makes falling behind visible.

## Alternatives rejected
- **The locally installed Go:** silently drifts out of support.
