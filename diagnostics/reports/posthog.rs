/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    cmp::Reverse,
    collections::HashMap,
    iter::Sum,
    ops::{Add, AddAssign},
    sync::Arc,
};

use resource::constants::diagnostics::{REPORT_MAX_INDIVIDUAL_DATABASES, REPORT_REMAINING_DATABASES_NAME};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::{
    DatabaseHash, DatabaseHashOpt, DatabaseId, Diagnostics, hash_string_consistently,
    metrics::{ALL_CLIENT_ENDPOINTS, ActionKind, ClientEndpoint},
    reports::{
        ActionReport, DataLoadReport, DatabaseReport, ErrorReport, LoadReport, OsReport, SchemaLoadReport,
        ServerPropertiesReport, ServerReport, ServerReportSensitivePart, ToJsonMap,
    },
};

const REPORTING_API_VERSION: usize = 2;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogPayload {
    pub api_key: String,
    pub batch: Vec<PosthogReport>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogReport {
    pub event: &'static str,
    pub properties: Map<String, Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogServerPropertiesReport {
    pub distinct_id: String,
    pub server_id: String,
    pub distribution: String,
    pub reporting_version: usize,
    pub enabled: bool,
}

impl From<ServerPropertiesReport> for PosthogServerPropertiesReport {
    fn from(value: ServerPropertiesReport) -> Self {
        Self {
            distinct_id: value.deployment_id,
            server_id: value.server_id,
            distribution: value.distribution,
            reporting_version: REPORTING_API_VERSION,
            enabled: value.enabled,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogServerReport {
    pub version: String,

    #[serde(flatten)]
    pub sensitive_part: Option<PosthogServerReportSensitivePart>,
}

impl From<ServerReport> for PosthogServerReport {
    fn from(value: ServerReport) -> Self {
        Self { version: value.version, sensitive_part: value.sensitive_part.map(|part| part.into()) }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogServerReportSensitivePart {
    pub uptime_in_seconds: i64,
    pub os: PosthogOsReport,
    pub memory_used_in_bytes: u64,
    pub memory_available_in_bytes: u64,
    pub disk_used_in_bytes: u64,
    pub disk_available_in_bytes: u64,
}

impl From<ServerReportSensitivePart> for PosthogServerReportSensitivePart {
    fn from(value: ServerReportSensitivePart) -> Self {
        Self {
            uptime_in_seconds: value.uptime_in_seconds,
            os: value.os.into(),
            memory_used_in_bytes: value.memory_used_in_bytes,
            memory_available_in_bytes: value.memory_available_in_bytes,
            disk_used_in_bytes: value.disk_used_in_bytes,
            disk_available_in_bytes: value.disk_available_in_bytes,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PosthogOsReport {
    pub name: String,
    pub arch: String,
    pub version: String,
}

impl From<OsReport> for PosthogOsReport {
    fn from(value: OsReport) -> Self {
        Self { name: value.name, arch: value.arch, version: value.version }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct PosthogLoadReport {
    schema: Option<PosthogSchemaLoadReport>,
    data: Option<PosthogDataLoadReport>,
    connection: PosthogConnectionLoadReport,
}

impl From<LoadReport> for PosthogLoadReport {
    fn from(value: LoadReport) -> Self {
        let mut connection = PosthogConnectionLoadReport::new();
        if let Some(connection_reports) = value.connection {
            for (client, report) in connection_reports {
                for (load_kind, count) in report {
                    let inner = connection.entry(load_kind.to_posthog_name().to_string()).or_insert_with(HashMap::new);
                    inner.insert(client, count);
                }
            }
        }

        Self { schema: value.schema.map(|schema| schema.into()), data: value.data.map(|data| data.into()), connection }
    }
}

impl PosthogLoadReport {
    // Connection counts are per-database peaks, so their sum is an upper bound of the combined peak.
    fn merge(&mut self, other: PosthogLoadReport) {
        self.schema = match (self.schema.take(), other.schema) {
            (Some(lhs), Some(rhs)) => Some(PosthogSchemaLoadReport { type_count: lhs.type_count + rhs.type_count }),
            (lhs, rhs) => lhs.or(rhs),
        };
        self.data = match (self.data.take(), other.data) {
            (Some(lhs), Some(rhs)) => Some(lhs + rhs),
            (lhs, rhs) => lhs.or(rhs),
        };
        for (load_kind, counts) in other.connection {
            let inner = self.connection.entry(load_kind).or_default();
            for (client, count) in counts {
                *inner.entry(client).or_default() += count;
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogDatabaseReport {
    database_hash: DatabaseHash,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogRemainingDatabasesReport {
    database_hash: DatabaseHash,
    aggregated_database_count: usize,
}

impl<T> From<T> for PosthogDatabaseReport
where
    T: Into<DatabaseReport>,
{
    fn from(value: T) -> Self {
        Self { database_hash: value.into().0.hash_value() }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogSchemaLoadReport {
    type_count: u64,
}

impl From<SchemaLoadReport> for PosthogSchemaLoadReport {
    fn from(value: SchemaLoadReport) -> Self {
        Self { type_count: value.type_count }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogDataLoadReport {
    entity_count: u64,
    relation_count: u64,
    attribute_count: u64,
    has_count: u64,
    role_count: u64,
    storage_in_bytes: u64,
    storage_key_count: u64,
}

impl Add for PosthogDataLoadReport {
    type Output = PosthogDataLoadReport;

    fn add(self, rhs: PosthogDataLoadReport) -> Self::Output {
        Self {
            entity_count: self.entity_count + rhs.entity_count,
            relation_count: self.relation_count + rhs.relation_count,
            attribute_count: self.attribute_count + rhs.attribute_count,
            has_count: self.has_count + rhs.has_count,
            role_count: self.role_count + rhs.role_count,
            storage_in_bytes: self.storage_in_bytes + rhs.storage_in_bytes,
            storage_key_count: self.storage_key_count + rhs.storage_key_count,
        }
    }
}

impl From<DataLoadReport> for PosthogDataLoadReport {
    fn from(value: DataLoadReport) -> Self {
        Self {
            entity_count: value.entity_count,
            relation_count: value.relation_count,
            attribute_count: value.attribute_count,
            has_count: value.has_count,
            role_count: value.role_count,
            storage_in_bytes: value.storage_in_bytes,
            storage_key_count: value.storage_key_count,
        }
    }
}

pub type PosthogConnectionLoadReport = HashMap<String, HashMap<ClientEndpoint, u64>>;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogActionReport {
    pub successful: i64,
    pub failed: i64,
}

impl PosthogActionReport {
    pub fn new() -> Self {
        Self { successful: 0, failed: 0 }
    }
}

impl From<ActionReport> for PosthogActionReport {
    fn from(value: ActionReport) -> Self {
        Self { successful: value.successful, failed: value.failed }
    }
}

impl Add<PosthogActionReport> for PosthogActionReport {
    type Output = PosthogActionReport;

    fn add(self, rhs: PosthogActionReport) -> Self::Output {
        PosthogActionReport { successful: self.successful + rhs.successful, failed: self.failed + rhs.failed }
    }
}

impl AddAssign<PosthogActionReport> for PosthogActionReport {
    fn add_assign(&mut self, rhs: PosthogActionReport) {
        self.successful += rhs.successful;
        self.failed += rhs.failed;
    }
}

impl Sum for PosthogActionReport {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::new(), |lhs, rhs| lhs + rhs)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct PosthogErrorReport {
    count: i64,
}

impl From<ErrorReport> for PosthogErrorReport {
    fn from(value: ErrorReport) -> Self {
        Self { count: value.count }
    }
}

type PosthogActionReports = HashMap<ActionKind, HashMap<ClientEndpoint, PosthogActionReport>>;
type PosthogErrorReports = HashMap<String, HashMap<ClientEndpoint, PosthogErrorReport>>;

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
enum PosthogReportKind {
    ServerUsage,
    DatabaseUsage,
}

impl PosthogReportKind {
    pub fn to_posthog_name(&self) -> &'static str {
        match self {
            PosthogReportKind::ServerUsage => "server_usage",
            PosthogReportKind::DatabaseUsage => "database_usage",
        }
    }
}

struct PosthogReportBuilder {
    kind: PosthogReportKind,
    pub flat_properties: Map<String, Value>,
    pub load_report: Option<PosthogLoadReport>,
    pub action_reports: PosthogActionReports,
    pub error_reports: PosthogErrorReports,
}

impl PosthogReportBuilder {
    pub fn server_usage() -> Self {
        Self::new(PosthogReportKind::ServerUsage)
    }

    pub fn database_usage() -> Self {
        Self::new(PosthogReportKind::DatabaseUsage)
    }

    fn new(kind: PosthogReportKind) -> Self {
        Self {
            kind,
            flat_properties: Map::new(),
            load_report: None,
            action_reports: PosthogActionReports::new(),
            error_reports: PosthogErrorReports::new(),
        }
    }

    pub fn set_load(&mut self, load_report: LoadReport) {
        self.load_report = Some(load_report.into());
    }

    pub fn insert_action(&mut self, client: ClientEndpoint, action_report: ActionReport) {
        let inner = self.action_reports.entry(action_report.kind).or_insert_with(HashMap::new);
        inner.insert(client, action_report.into());
    }

    pub fn insert_error(&mut self, client: ClientEndpoint, error_report: ErrorReport) {
        let inner = self.error_reports.entry(error_report.code.clone()).or_insert_with(HashMap::new);
        inner.insert(client, error_report.into());
    }

    /// Sums the load, action and error metrics of `other` into `self`. Flat properties of `other` are dropped.
    fn merge(&mut self, other: PosthogReportBuilder) {
        if let Some(other_load) = other.load_report {
            match &mut self.load_report {
                Some(load) => load.merge(other_load),
                None => self.load_report = Some(other_load),
            }
        }
        for (action_kind, reports) in other.action_reports {
            let inner = self.action_reports.entry(action_kind).or_default();
            for (client, report) in reports {
                *inner.entry(client).or_insert_with(PosthogActionReport::new) += report;
            }
        }
        for (code, reports) in other.error_reports {
            let inner = self.error_reports.entry(code).or_default();
            for (client, report) in reports {
                inner.entry(client).or_insert(PosthogErrorReport { count: 0 }).count += report.count;
            }
        }
    }

    /// Ordering key for picking the most active databases: queries in this reporting interval,
    /// then all other actions (e.g. transaction opens), then storage size.
    fn activity_key(&self) -> (i64, i64, u64) {
        let mut queries = 0;
        let mut actions = 0;
        for (action_kind, reports) in &self.action_reports {
            let count: i64 = reports.values().map(|report| report.successful + report.failed).sum();
            actions += count;
            if action_kind.is_query() {
                queries += count;
            }
        }
        let storage =
            self.load_report.as_ref().and_then(|load| load.data.as_ref()).map_or(0, |data| data.storage_in_bytes);
        (queries, actions, storage)
    }

    fn build(self) -> PosthogReport {
        let mut properties = self.flat_properties;

        if let Some(load_report) = self.load_report {
            properties.extend(load_report.to_json_map());
        }

        let mut total_queries = PosthogActionReport::new();
        for (action_kind, report) in self.action_reports {
            total_queries += Self::get_total_queries(action_kind, &report);
            properties.insert(action_kind.to_posthog_name().to_string(), json!(report));
        }

        if matches!(self.kind, PosthogReportKind::DatabaseUsage) {
            properties.insert("total_queries".to_string(), json!(total_queries));
        }

        if !self.error_reports.is_empty() {
            properties.insert("user_errors".to_string(), json!(self.error_reports));
        }

        PosthogReport { event: self.kind.to_posthog_name(), properties }
    }

    fn get_total_queries(
        action_kind: ActionKind,
        report: &HashMap<ClientEndpoint, PosthogActionReport>,
    ) -> PosthogActionReport {
        match action_kind.is_query() {
            true => report.values().cloned().sum(),
            false => PosthogActionReport::new(),
        }
    }
}

struct PosthogPayloadBuilder {
    api_key: String,
    usage_reports: HashMap<DatabaseHashOpt, PosthogReportBuilder>,
    server_properties: Map<String, Value>,
    server_metrics_full: Map<String, Value>,
    server_metrics_minimal: Map<String, Value>,
}

impl PosthogPayloadBuilder {
    fn new(diagnostics: &Diagnostics, api_key: String) -> Self {
        let core_metrics = diagnostics.core_metrics();
        let server_properties: PosthogServerPropertiesReport = core_metrics.server_properties.to_state_report().into();
        let server_metrics_full: PosthogServerReport = core_metrics.server_metrics.to_full_state_report().into();
        let server_metrics_minimal: PosthogServerReport = core_metrics.server_metrics.to_minimal_state_report().into();
        Self {
            api_key,
            usage_reports: HashMap::new(),
            server_properties: server_properties.to_json_map(),
            server_metrics_full: server_metrics_full.to_json_map(),
            server_metrics_minimal: server_metrics_minimal.to_json_map(),
        }
    }

    fn build(self) -> PosthogPayload {
        let batch = if self.usage_reports.is_empty() {
            let mut server_usage = PosthogReportBuilder::server_usage();
            server_usage.flat_properties.extend(self.server_properties.to_json_map());
            server_usage.flat_properties.extend(self.server_metrics_minimal);
            vec![server_usage.build()]
        } else {
            let mut builders = Vec::with_capacity(self.usage_reports.len());
            let mut database_builders = Vec::new();
            for (database_hash, builder) in self.usage_reports {
                match database_hash {
                    None => builders.push(builder),
                    Some(database_hash) => database_builders.push((database_hash, builder)),
                }
            }

            // Servers can host an unbounded number of databases, so only report the most active ones
            // individually and sum up the rest. The hash breaks ties deterministically.
            if database_builders.len() > REPORT_MAX_INDIVIDUAL_DATABASES {
                database_builders
                    .sort_by_cached_key(|(database_hash, builder)| (Reverse(builder.activity_key()), *database_hash));
                let remaining = database_builders.split_off(REPORT_MAX_INDIVIDUAL_DATABASES);
                builders.push(Self::remaining_databases_report(
                    remaining.into_iter().map(|(_, builder)| builder).collect(),
                    &self.server_properties,
                    &self.server_metrics_minimal,
                ));
            }
            builders.extend(database_builders.into_iter().map(|(_, builder)| builder));
            builders.into_iter().map(|builder| builder.build()).collect()
        };

        PosthogPayload { api_key: self.api_key, batch }
    }

    fn remaining_databases_report(
        remaining: Vec<PosthogReportBuilder>,
        server_properties: &Map<String, Value>,
        server_metrics_minimal: &Map<String, Value>,
    ) -> PosthogReportBuilder {
        let mut summary = PosthogReportBuilder::database_usage();
        let report = PosthogRemainingDatabasesReport {
            database_hash: hash_string_consistently(REPORT_REMAINING_DATABASES_NAME),
            aggregated_database_count: remaining.len(),
        };
        summary.flat_properties.extend(report.to_json_map());
        summary.flat_properties.extend(server_metrics_minimal.clone());
        summary.flat_properties.extend(server_properties.clone());
        for builder in remaining {
            summary.merge(builder);
        }
        summary
    }

    fn get_or_init_report(&mut self, database_id: Option<Arc<DatabaseId>>) -> &mut PosthogReportBuilder {
        let database_hash = database_id.as_ref().map(|id| id.hash_value());
        self.init_report_if_needed(database_id);
        self.usage_reports.get_mut(&database_hash).expect("Expected to get by a just inserted key")
    }

    fn init_report_if_needed(&mut self, database_id: Option<Arc<DatabaseId>>) {
        let database_hash = database_id.as_ref().map(|id| id.hash_value());
        if !self.usage_reports.contains_key(&database_hash) {
            let mut usage = if let Some(database_id) = database_id {
                let mut database_usage = PosthogReportBuilder::database_usage();
                database_usage.flat_properties.extend(PosthogDatabaseReport::from(database_id).to_json_map());
                database_usage.flat_properties.extend(self.server_metrics_minimal.clone());
                database_usage
            } else {
                let mut server_usage = PosthogReportBuilder::server_usage();
                server_usage.flat_properties.extend(self.server_metrics_full.clone());
                server_usage
            };
            usage.flat_properties.extend(self.server_properties.clone());
            self.usage_reports.insert(database_hash, usage);
        }
    }
}

pub(crate) fn to_full_posthog_reporting_json(diagnostics: &Diagnostics, api_key: &str) -> Value {
    let mut builder = PosthogPayloadBuilder::new(diagnostics, api_key.to_string());
    builder.init_report_if_needed(None);

    for metrics in diagnostics.core_metrics().lock_load_metrics_read().values() {
        match metrics.to_peak_report() {
            Some(load_report) => {
                let report_builder = builder.get_or_init_report(Some(metrics.database_id().clone()));
                report_builder.set_load(load_report);
            }
            None => continue,
        }
    }

    for client in ALL_CLIENT_ENDPOINTS {
        for metrics in diagnostics.core_metrics().lock_action_metrics_read(client).values() {
            let action_reports = metrics.to_diff_reports();
            if action_reports.is_empty() {
                continue;
            }

            let report_builder = builder.get_or_init_report(metrics.database_id().cloned());
            for action_report in action_reports {
                report_builder.insert_action(client, action_report);
            }
        }

        for metrics in diagnostics.core_metrics().lock_error_metrics_read(client).values() {
            let error_reports = metrics.to_diff_reports();
            if error_reports.is_empty() {
                continue;
            }

            let report_builder = builder.get_or_init_report(metrics.database_id().cloned());
            for error_report in error_reports {
                report_builder.insert_error(client, error_report);
            }
        }
    }

    json!(builder.build())
}

pub(crate) fn to_minimal_posthog_reporting_json(diagnostics: &Diagnostics, api_key: &str) -> Value {
    json!(PosthogPayloadBuilder::new(diagnostics, api_key.to_string()).build())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use resource::constants::diagnostics::{REPORT_MAX_INDIVIDUAL_DATABASES, REPORT_REMAINING_DATABASES_NAME};
    use serde_json::Value;

    use crate::{
        Diagnostics, hash_string_consistently,
        metrics::{ActionKind, ClientEndpoint, DataLoadMetrics, DatabaseMetricsSnapshot, SchemaLoadMetrics},
    };

    fn diagnostics_with_databases(database_count: usize) -> Diagnostics {
        let diagnostics = Diagnostics::new(
            "deploy".into(),
            "server".into(),
            "edition".into(),
            "version".into(),
            PathBuf::from("/tmp/diagnostics-test"),
            /* is_reporting_enabled */ true,
            /* metrics_enabled */ true,
        );
        let mut snapshots = HashMap::new();
        for i in 0..database_count {
            let name: Arc<str> = format!("db_{i}").into();
            snapshots.insert(
                name.clone(),
                DatabaseMetricsSnapshot {
                    schema: SchemaLoadMetrics { type_count: 1 },
                    data: DataLoadMetrics { entity_count: 10, ..Default::default() },
                },
            );
            // db_i runs i queries, so the highest indices are the most active
            for _ in 0..i {
                diagnostics.submit_action_success(ClientEndpoint::Grpc, Some(&name), ActionKind::OneshotQuery);
            }
            diagnostics.submit_error(ClientEndpoint::Http, Some(&name), "ERR-1".into());
        }
        diagnostics.submit_database_metrics(snapshots);
        diagnostics
    }

    fn database_events(payload: &Value) -> Vec<&Value> {
        payload["batch"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["event"] == "database_usage")
            .map(|event| &event["properties"])
            .collect()
    }

    fn summary_hash() -> u64 {
        hash_string_consistently(REPORT_REMAINING_DATABASES_NAME)
    }

    #[test]
    fn databases_under_cap_are_reported_individually() {
        let diagnostics = diagnostics_with_databases(REPORT_MAX_INDIVIDUAL_DATABASES);
        let payload = diagnostics.to_posthog_reporting_json_against_snapshot("test-key");
        let events = database_events(&payload);

        assert_eq!(events.len(), REPORT_MAX_INDIVIDUAL_DATABASES);
        assert!(events.iter().all(|event| event["database_hash"] != summary_hash()));
        assert!(events.iter().all(|event| event.get("aggregated_database_count").is_none()));
    }

    #[test]
    fn databases_over_cap_are_summarised() {
        let database_count = REPORT_MAX_INDIVIDUAL_DATABASES + 5;
        let diagnostics = diagnostics_with_databases(database_count);
        let payload = diagnostics.to_posthog_reporting_json_against_snapshot("test-key");
        let events = database_events(&payload);

        assert_eq!(events.len(), REPORT_MAX_INDIVIDUAL_DATABASES + 1);
        assert_eq!(payload["batch"].as_array().unwrap().len(), REPORT_MAX_INDIVIDUAL_DATABASES + 2); // + server_usage

        // The most active databases are reported individually
        for i in (database_count - REPORT_MAX_INDIVIDUAL_DATABASES)..database_count {
            let hash = hash_string_consistently(format!("db_{i}"));
            let event = events.iter().find(|event| event["database_hash"] == hash).expect("missing top database");
            assert_eq!(event["total_queries"]["successful"], i as i64);
        }

        // The rest are summed into one event
        let summary = events.iter().find(|event| event["database_hash"] == summary_hash()).expect("missing summary");
        let remaining_count = database_count - REPORT_MAX_INDIVIDUAL_DATABASES;
        let remaining_queries: usize = (0..remaining_count).sum();
        assert_eq!(summary["aggregated_database_count"], remaining_count);
        assert_eq!(summary["total_queries"]["successful"], remaining_queries);
        assert_eq!(summary["oneshot_queries"]["grpc"]["successful"], remaining_queries);
        assert_eq!(summary["schema"]["type_count"], remaining_count);
        assert_eq!(summary["data"]["entity_count"], remaining_count * 10);
        assert_eq!(summary["user_errors"]["ERR-1"]["http"]["count"], remaining_count);
        assert_eq!(summary["distinct_id"], "deploy");
    }
}
