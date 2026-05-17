/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/// Engine-internal options consumed by `QueryManager` when executing schema, read, or
/// write queries. Distinct from `options::ServerQueryOptions` (which carries
/// protocol-facing concerns like prefetch size and instance-type inclusion).
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct QueryOptions {
    /// Force enable the runtime `QueryProfile` even when `tracing` is not at TRACE level.
    /// Used by tests/benchmarks that need to inspect step-level profile data; production
    /// callers leave this `false` and rely on the `tracing::enabled!(Level::TRACE)` gate.
    pub force_query_profile: bool,
}
