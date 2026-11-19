// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use reqwest::StatusCode;
use url::Url;

/// Upstream health paths probed in order. Dynamo-style engines serve `/health`;
/// OpenAI-style NIM images serve `/v1/health/ready` and no `/health`.
pub const DEFAULT_UPSTREAM_HEALTH_PATHS: [&str; 2] = ["/health", "/v1/health/ready"];

/// Health path NVCF stores for a function that declares no health check.
const CONTROL_PLANE_DEFAULT_HEALTH_PATH: &str = "/v2/health/ready";

const UNRESOLVED: usize = usize::MAX;

#[derive(Debug, thiserror::Error)]
pub enum ReadinessGateError {
    #[error("invalid upstream URL for the readiness gate: {0}")]
    Url(#[from] url::ParseError),
    #[error("readiness gate needs a health path")]
    EmptyPath,
    #[error("upstream URL {0} cannot take a health port")]
    PortNotSupported(String),
    #[error("readiness gate expected status {0} is not an HTTP status code")]
    InvalidStatus(u16),
}

/// The function's declared HTTP health check. Unlike [`UpstreamHealthPaths`],
/// only the expected status passes, as in the NVCF `utils` worker.
#[derive(Clone, Debug)]
pub struct ReadinessGate {
    url: String,
    expected_status: StatusCode,
    // Backends that do not serve the control-plane default answer it with 404.
    // Those functions declared nothing, so they keep the lenient probe.
    is_control_plane_default: bool,
    fallback_logged: Arc<AtomicBool>,
}

impl ReadinessGate {
    /// `port` is the declared health port; `None` uses the upstream's port.
    pub fn new(
        upstream_http_base_url: &str,
        path: &str,
        port: Option<u16>,
        expected_status: u16,
    ) -> Result<Self, ReadinessGateError> {
        let path = normalize_path(path);
        if path.is_empty() {
            return Err(ReadinessGateError::EmptyPath);
        }
        let expected_status = StatusCode::from_u16(expected_status)
            .map_err(|_| ReadinessGateError::InvalidStatus(expected_status))?;
        let mut url = Url::parse(upstream_http_base_url)?;
        let upstream_port = url.port_or_known_default();
        let port = port.or(upstream_port);
        url.set_port(port).map_err(|()| {
            ReadinessGateError::PortNotSupported(upstream_http_base_url.to_string())
        })?;
        url.set_path(&path);
        Ok(Self {
            url: url.into(),
            expected_status,
            is_control_plane_default: path == CONTROL_PLANE_DEFAULT_HEALTH_PATH
                && port == upstream_port,
            fallback_logged: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn expected_status(&self) -> StatusCode {
        self.expected_status
    }

    /// Whether a 404 falls back to the lenient probe; logs the first fallback.
    pub(crate) fn not_found_falls_back(&self) -> bool {
        if !self.is_control_plane_default {
            return false;
        }
        if !self.fallback_logged.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                url = %self.url,
                "upstream does not serve the default health check; using the fallback health paths"
            );
        }
        true
    }
}

#[derive(Clone, Debug)]
pub struct UpstreamHealthPaths {
    candidates: Arc<Vec<String>>,
    resolved: Arc<AtomicUsize>,
}

impl Default for UpstreamHealthPaths {
    fn default() -> Self {
        Self::new(DEFAULT_UPSTREAM_HEALTH_PATHS)
    }
}

impl UpstreamHealthPaths {
    /// Configured paths are probed first; the defaults always stay as a
    /// fallback so a wrong path cannot strand an otherwise healthy upstream.
    pub fn new<I, P>(paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<str>,
    {
        let mut candidates = Vec::new();
        let configured = paths.into_iter().map(|path| normalize_path(path.as_ref()));
        let defaults = DEFAULT_UPSTREAM_HEALTH_PATHS
            .iter()
            .map(|path| (*path).to_string());
        for path in configured.chain(defaults) {
            if !path.is_empty() && !candidates.contains(&path) {
                candidates.push(path);
            }
        }
        Self {
            candidates: Arc::new(candidates),
            resolved: Arc::new(AtomicUsize::new(UNRESOLVED)),
        }
    }

    pub fn resolved_path(&self) -> Option<String> {
        self.resolved_index()
            .map(|index| self.candidates[index].clone())
    }

    /// Path a health probe should use right now: the resolved one, or the first
    /// candidate before any probe has succeeded.
    pub fn probe_path(&self) -> &str {
        let index = self.resolved_index().unwrap_or(0);
        &self.candidates[index]
    }

    pub(crate) fn probe_order(&self) -> Vec<(usize, &str)> {
        let resolved = self.resolved_index();
        let mut order: Vec<(usize, &str)> = resolved
            .map(|index| vec![(index, self.candidates[index].as_str())])
            .unwrap_or_default();
        order.extend(
            self.candidates
                .iter()
                .enumerate()
                .filter(|(index, _)| Some(*index) != resolved)
                .map(|(index, path)| (index, path.as_str())),
        );
        order
    }

    pub(crate) fn mark_resolved(&self, index: usize) {
        self.resolved.store(index, Ordering::Relaxed);
    }

    fn resolved_index(&self) -> Option<usize> {
        match self.resolved.load(Ordering::Relaxed) {
            UNRESOLVED => None,
            index => Some(index),
        }
    }
}

fn normalize_path(path: &str) -> String {
    let path = path.trim();
    if path.is_empty() {
        return String::new();
    }
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_and_deduplicates_candidates() {
        let paths = UpstreamHealthPaths::new([" health ", "/health", "v1/health/ready", ""]);

        assert_eq!(
            paths.probe_order(),
            vec![(0, "/health"), (1, "/v1/health/ready")]
        );
    }

    #[test]
    fn falls_back_to_the_defaults_when_no_path_is_configured() {
        let paths = UpstreamHealthPaths::new(Vec::<String>::new());

        assert_eq!(paths.probe_path(), "/health");
    }

    #[test]
    fn configured_paths_are_probed_before_the_defaults() {
        let paths = UpstreamHealthPaths::new(["/ping"]);

        assert_eq!(
            paths.probe_order(),
            vec![(0, "/ping"), (1, "/health"), (2, "/v1/health/ready")]
        );
        assert_eq!(paths.probe_path(), "/ping");
    }

    #[test]
    fn readiness_gate_targets_the_declared_port_and_path() {
        for (label, path, port, expected_url, is_default) in [
            (
                "sidecar port",
                "/ready",
                Some(8001),
                "http://frontend:8001/ready",
                false,
            ),
            (
                "upstream port",
                "v1/health/ready",
                None,
                "http://frontend:8000/v1/health/ready",
                false,
            ),
            (
                "default check",
                "/v2/health/ready",
                None,
                "http://frontend:8000/v2/health/ready",
                true,
            ),
            (
                "default path, explicit same port",
                "/v2/health/ready",
                Some(8000),
                "http://frontend:8000/v2/health/ready",
                true,
            ),
            (
                "default path, other port",
                "/v2/health/ready",
                Some(8001),
                "http://frontend:8001/v2/health/ready",
                false,
            ),
        ] {
            let gate = ReadinessGate::new("http://frontend:8000", path, port, 200).expect(label);

            assert_eq!(gate.url(), expected_url, "{label}");
            assert_eq!(gate.not_found_falls_back(), is_default, "{label}");
        }
    }

    #[test]
    fn readiness_gate_rejects_an_empty_path_and_an_invalid_status() {
        assert!(matches!(
            ReadinessGate::new("http://frontend:8000", " ", None, 200),
            Err(ReadinessGateError::EmptyPath)
        ));
        assert!(matches!(
            ReadinessGate::new("http://frontend:8000", "/ready", None, 1000),
            Err(ReadinessGateError::InvalidStatus(1000))
        ));
    }

    #[test]
    fn probes_the_resolved_path_first() {
        let paths = UpstreamHealthPaths::default();
        paths.mark_resolved(1);

        assert_eq!(
            paths.probe_order(),
            vec![(1, "/v1/health/ready"), (0, "/health")]
        );
        assert_eq!(paths.probe_path(), "/v1/health/ready");
    }
}
