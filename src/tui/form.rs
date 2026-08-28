//! Interactive form modal for collecting task param values.
//!
//! Opened by the task table when the user selects a task with declared
//! [`params`](crate::config::TaskParam). Owns the value for each declared
//! param, the focus index, and any in-flight completion-resolve state.
//!
//! The form takes over the side panel, like the status tables and the filter
//! — the log keeps flowing beside it, because the answer to a param is often
//! sitting in it. One field per param, rendered top-to-bottom. Tab/Shift-Tab
//! navigates between fields; Enter advances (or submits on the last field);
//! Esc hands the panel back to the task table.
//!
//! ## Widget per kind
//!
//! - `String` / `Choice` — text input with optional fuzzy-filtered dropdown.
//!   Static `choices` list in the dropdown immediately; dynamic
//!   `completions` trigger [`RunnerCommand::ResolveCompletions`] on focus,
//!   refreshable via Tab.
//! - `Int` — text input that accepts digits + `-`, with ↑/↓ stepper and
//!   commit-time validation against `validate.min`/`validate.max`.
//! - `Bool` — toggle. Space (or Enter) flips it; no dropdown.
//!
//! ## Completion failure
//!
//! When the completion command fails, the form renders a red banner beneath
//! the field pointing at the log file the runner wrote. The field stays
//! usable as free-text — the user can still type an answer and submit.

use std::collections::HashMap;

use crate::client::CompletionError;
use crate::config::{ParamKind, Task, TaskParam};

use super::fuzzy::fuzzy_match;

/// State for a single field in the form.
#[derive(Debug, Clone)]
pub(crate) struct Field {
    /// Param name (= flag name on the CLI).
    pub(crate) name: String,
    /// Prompt text shown above the input. Defaults to `name`.
    pub(crate) prompt: String,
    /// Whether the user must supply a value (no default).
    pub(crate) required: bool,
    /// Param kind — drives the widget and validation.
    pub(crate) kind: ParamKind,
    /// Current text value. For `Bool`, canonicalized to `"true"` or
    /// `"false"`. For `Int`, the raw typed string (validated on submit).
    pub(crate) value: String,
    /// Static `choices` from the config. Present for `Choice` kind or for
    /// `String` params with a `choices` list.
    pub(crate) static_choices: Vec<String>,
    /// Whether the param has a `completions` block (dynamic candidates).
    pub(crate) has_dynamic_completions: bool,
    /// Current dropdown state. Populated from `static_choices` on open and
    /// overwritten when a `CompletionsReady` arrives.
    pub(crate) candidates: CandidateState,
    /// Highlighted candidate index in the *filtered* view. Clamped at
    /// render time against the visible slice.
    pub(crate) candidate_highlight: usize,
    /// Error shown beneath the field (validation failure, completion error).
    pub(crate) error: Option<String>,
    /// Min/max bounds for `Int` kind, copied from `validate`.
    pub(crate) int_min: Option<i64>,
    pub(crate) int_max: Option<i64>,
}

/// Per-field candidate state — whether we have completions loaded and if
/// not, why.
#[derive(Debug, Clone)]
pub(crate) enum CandidateState {
    /// No completion source declared — plain text field.
    None,
    /// Static `choices` from the config. Always available.
    Static(Vec<String>),
    /// Dynamic completions loading (request in flight).
    Loading,
    /// Dynamic completions loaded.
    Loaded(Vec<String>),
    /// Dynamic completions failed. The banner below the field surfaces
    /// `message`; `log_path` is shown verbatim.
    Failed {
        message: String,
        log_path: Option<std::path::PathBuf>,
    },
}

impl CandidateState {
    /// Return the underlying candidate list for fuzzy filtering.
    /// `None`/`Loading`/`Failed` all return an empty slice — the renderer
    /// uses the variant tag to decide what to show in the dropdown area.
    pub(crate) fn list(&self) -> &[String] {
        match self {
            CandidateState::Static(v) | CandidateState::Loaded(v) => v,
            _ => &[],
        }
    }
}

/// Visible window into a field's filtered candidate list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateWindow {
    /// Candidates currently visible in the dropdown.
    pub(crate) items: Vec<String>,
    /// Highlight index relative to `items`.
    pub(crate) highlight: usize,
    /// Number of filtered candidates above the visible window.
    pub(crate) hidden_above: usize,
    /// Number of filtered candidates below the visible window.
    pub(crate) hidden_below: usize,
}

/// Top-level form state, one instance lives on [`super::app::App`] while
/// the form modal is open.
#[derive(Debug, Clone)]
pub(crate) struct FormState {
    /// Task the form is collecting params for.
    pub(crate) task: String,
    /// Fields in declaration order.
    pub(crate) fields: Vec<Field>,
    /// Index of the focused field.
    pub(crate) focus: usize,
    /// Monotonic counter bumped on each `ResolveCompletions` request —
    /// stale replies are dropped by comparing against the current value.
    pub(crate) request_counter: u64,
    /// Map of `param name -> request_id` that the form cares about. A
    /// `CompletionsReady` event with an older id than the one stored here
    /// for its param is discarded.
    pub(crate) in_flight: HashMap<String, u64>,
    /// Top-level submit error (e.g. required missing, validation failed).
    /// Rendered in the modal's footer above the key hints.
    pub(crate) submit_error: Option<String>,
}

impl FormState {
    /// Build a form for `task` using its config. Seeds each field with its
    /// default value (empty string when absent). Returns `None` when the
    /// task has no declared params — callers should have already guarded
    /// against this before opening the form.
    pub(crate) fn new(task_name: &str, task: &Task) -> Option<Self> {
        if task.params.is_empty() {
            return None;
        }
        let fields = task.params.iter().map(Field::from_param).collect();
        Some(Self {
            task: task_name.to_string(),
            fields,
            focus: 0,
            request_counter: 0,
            in_flight: HashMap::new(),
            submit_error: None,
        })
    }

    pub(crate) fn focused(&self) -> Option<&Field> {
        self.fields.get(self.focus)
    }

    pub(crate) fn focused_mut(&mut self) -> Option<&mut Field> {
        self.fields.get_mut(self.focus)
    }

    /// Move focus to the next field, wrapping at the end.
    pub(crate) fn focus_next(&mut self) {
        if self.fields.is_empty() {
            return;
        }
        self.focus = (self.focus + 1) % self.fields.len();
    }

    /// Move focus to the previous field, wrapping at the start.
    pub(crate) fn focus_prev(&mut self) {
        if self.fields.is_empty() {
            return;
        }
        self.focus = if self.focus == 0 {
            self.fields.len() - 1
        } else {
            self.focus - 1
        };
    }

    /// Allocate a new request id and mark the param as "in flight".
    pub(crate) fn start_request(&mut self, param: &str) -> u64 {
        self.request_counter += 1;
        let id = self.request_counter;
        self.in_flight.insert(param.to_string(), id);
        if let Some(f) = self.fields.iter_mut().find(|f| f.name == param) {
            f.candidates = CandidateState::Loading;
            f.error = None;
        }
        id
    }

    /// Apply a `CompletionsReady` event. Drops stale replies (where the
    /// request id doesn't match the one we last issued for this param).
    pub(crate) fn apply_completions(
        &mut self,
        param: &str,
        request_id: u64,
        result: Result<Vec<String>, CompletionError>,
    ) {
        match self.in_flight.get(param) {
            Some(&id) if id == request_id => {}
            _ => return, // stale
        }
        self.in_flight.remove(param);
        let Some(field) = self.fields.iter_mut().find(|f| f.name == param) else {
            return;
        };
        field.candidates = match result {
            Ok(values) => CandidateState::Loaded(values),
            Err(e) => CandidateState::Failed {
                message: e.message,
                log_path: e.log_path,
            },
        };
        field.candidate_highlight = 0;
    }

    /// Prepare the `params` map for [`RunnerCommand::RunTask`]. Validates
    /// required-and-empty fields and int bounds; returns
    /// `Err(user_facing_message)` if validation fails — callers should
    /// store it in `submit_error` and keep the form open.
    pub(crate) fn submit(&self) -> Result<HashMap<String, String>, String> {
        let mut out = HashMap::new();
        for field in &self.fields {
            let v = field.value.trim();
            if v.is_empty() {
                if field.required {
                    return Err(format!("'{}' is required", field.name));
                }
                continue;
            }
            match field.kind {
                ParamKind::Int => {
                    let n: i64 = v
                        .parse()
                        .map_err(|_| format!("'{}' must be an integer (got '{v}')", field.name))?;
                    if let Some(min) = field.int_min
                        && n < min
                    {
                        return Err(format!("'{}' must be >= {min} (got {n})", field.name));
                    }
                    if let Some(max) = field.int_max
                        && n > max
                    {
                        return Err(format!("'{}' must be <= {max} (got {n})", field.name));
                    }
                    out.insert(field.name.clone(), n.to_string());
                }
                ParamKind::Bool => {
                    // value is already canonicalized to "true"/"false" by
                    // the key handler.
                    out.insert(field.name.clone(), v.to_string());
                }
                _ => {
                    // Static choices are enforced here: if the param has a
                    // fixed set and the value is outside it, reject.
                    if !field.static_choices.is_empty()
                        && !field.static_choices.iter().any(|c| c == v)
                    {
                        return Err(format!(
                            "'{}' must be one of [{}]",
                            field.name,
                            field.static_choices.join(", ")
                        ));
                    }
                    out.insert(field.name.clone(), v.to_string());
                }
            }
        }
        Ok(out)
    }
}

impl Field {
    fn from_param(p: &TaskParam) -> Self {
        let prompt = p.prompt.clone().unwrap_or_else(|| p.name.clone());
        let value = p.default.clone().unwrap_or_default();
        let static_choices = p.choices.clone();
        let has_dynamic_completions = p.completions.is_some();
        let candidates = if !static_choices.is_empty() {
            CandidateState::Static(static_choices.clone())
        } else if has_dynamic_completions {
            // The caller kicks off the first fetch after insertion — we
            // start in `None` rather than `Loading` so a form with many
            // dynamic fields doesn't show an animated spinner on every
            // one while only the focused field is actually loading.
            CandidateState::None
        } else {
            CandidateState::None
        };
        let (int_min, int_max) = match p.validate {
            Some(v) => (v.min, v.max),
            None => (None, None),
        };
        Self {
            name: p.name.clone(),
            prompt,
            required: p.required,
            kind: p.kind,
            value,
            static_choices,
            has_dynamic_completions,
            candidates,
            candidate_highlight: 0,
            error: None,
            int_min,
            int_max,
        }
    }

    /// Returns the filtered candidate list against the current value.
    /// Used by the renderer and the candidate-highlight Up/Down keys.
    pub(crate) fn visible_candidates(&self) -> Vec<String> {
        let list = self.candidates.list();
        if list.is_empty() {
            return Vec::new();
        }
        if self.value.is_empty() {
            return list.to_vec();
        }
        fuzzy_match(&self.value, list)
    }

    /// Return a scrollable window into the filtered candidates, keeping the
    /// highlighted item visible when there are more matches than fit.
    pub(crate) fn visible_candidate_window(&self, max_rows: usize) -> CandidateWindow {
        let visible = self.visible_candidates();
        if visible.is_empty() || max_rows == 0 {
            return CandidateWindow {
                items: Vec::new(),
                highlight: 0,
                hidden_above: 0,
                hidden_below: 0,
            };
        }

        let highlight = self.candidate_highlight.min(visible.len() - 1);
        let window_rows = max_rows.min(visible.len());
        let start = if highlight < window_rows {
            0
        } else {
            highlight + 1 - window_rows
        };
        let end = start + window_rows;
        CandidateWindow {
            items: visible[start..end].to_vec(),
            highlight: highlight - start,
            hidden_above: start,
            hidden_below: visible.len() - end,
        }
    }

    /// Accept the currently highlighted candidate into `value`.
    pub(crate) fn accept_highlighted_candidate(&mut self) {
        let visible = self.visible_candidates();
        if visible.is_empty() {
            return;
        }
        let idx = self.candidate_highlight.min(visible.len() - 1);
        self.value = visible[idx].clone();
    }

    /// Step an int value by `delta`, clamping against `int_min`/`int_max`.
    /// No-op when the current value isn't a valid int.
    pub(crate) fn step_int(&mut self, delta: i64) {
        if !matches!(self.kind, ParamKind::Int) {
            return;
        }
        let base: i64 = self.value.trim().parse().unwrap_or(0);
        let mut next = base.saturating_add(delta);
        if let Some(min) = self.int_min {
            next = next.max(min);
        }
        if let Some(max) = self.int_max {
            next = next.min(max);
        }
        self.value = next.to_string();
    }

    /// Flip a bool value. Initializes to `"true"` when empty.
    pub(crate) fn toggle_bool(&mut self) {
        if !matches!(self.kind, ParamKind::Bool) {
            return;
        }
        self.value = match self.value.trim() {
            "true" => "false".into(),
            _ => "true".into(),
        };
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::unnecessary_get_then_check)]
mod tests {
    use super::*;
    use crate::config::{LogConfig, ParamKind, ParamValidate, TaskParam};

    fn task_with(params: Vec<TaskParam>) -> Task {
        Task {
            cmd: "echo".into(),
            args: vec![],
            dir: None,
            env: HashMap::new(),
            depends_on: vec![],
            watch: vec![],
            ignore: vec![],
            debounce: None,
            timeout: None,
            log: LogConfig::Stdout,
            interactive: false,
            headless: None,
            auto_run: crate::config::TaskAutoRun::Always,
            download: None,
            bazel: None,
            params,
            hidden: false,
            auto_filter_on_failure: None,
        }
    }

    fn string_param(name: &str) -> TaskParam {
        TaskParam {
            name: name.into(),
            prompt: None,
            required: false,
            default: None,
            kind: ParamKind::String,
            choices: vec![],
            completions: None,
            validate: None,
        }
    }

    #[test]
    fn new_returns_none_for_empty_params() {
        assert!(FormState::new("t", &task_with(vec![])).is_none());
    }

    #[test]
    fn new_seeds_defaults_and_static_choices() {
        let task = task_with(vec![
            TaskParam {
                default: Some("hello".into()),
                ..string_param("a")
            },
            TaskParam {
                choices: vec!["x".into(), "y".into()],
                kind: ParamKind::Choice,
                ..string_param("b")
            },
        ]);
        let form = FormState::new("t", &task).unwrap();
        assert_eq!(form.fields[0].value, "hello");
        assert_eq!(form.fields[1].value, "");
        match &form.fields[1].candidates {
            CandidateState::Static(v) => assert_eq!(v, &vec!["x".to_string(), "y".to_string()]),
            other => panic!("expected Static, got {other:?}"),
        }
    }

    #[test]
    fn focus_wraps() {
        let task = task_with(vec![string_param("a"), string_param("b")]);
        let mut form = FormState::new("t", &task).unwrap();
        form.focus_next();
        assert_eq!(form.focus, 1);
        form.focus_next();
        assert_eq!(form.focus, 0);
        form.focus_prev();
        assert_eq!(form.focus, 1);
    }

    #[test]
    fn submit_requires_required_fields() {
        let task = task_with(vec![TaskParam {
            required: true,
            ..string_param("a")
        }]);
        let form = FormState::new("t", &task).unwrap();
        let err = form.submit().unwrap_err();
        assert!(err.contains("required"), "{err}");
    }

    #[test]
    fn submit_skips_empty_optional_fields() {
        let task = task_with(vec![string_param("a"), string_param("b")]);
        let mut form = FormState::new("t", &task).unwrap();
        form.fields[0].value = "hi".into();
        let got = form.submit().unwrap();
        assert_eq!(got.get("a").unwrap(), "hi");
        assert!(got.get("b").is_none());
    }

    #[test]
    fn submit_validates_int_bounds() {
        let task = task_with(vec![TaskParam {
            kind: ParamKind::Int,
            validate: Some(ParamValidate {
                min: Some(1),
                max: Some(10),
            }),
            ..string_param("n")
        }]);
        let mut form = FormState::new("t", &task).unwrap();

        form.fields[0].value = "5".into();
        assert!(form.submit().is_ok());

        form.fields[0].value = "0".into();
        assert!(form.submit().unwrap_err().contains(">= 1"));

        form.fields[0].value = "11".into();
        assert!(form.submit().unwrap_err().contains("<= 10"));

        form.fields[0].value = "abc".into();
        assert!(form.submit().unwrap_err().contains("integer"));
    }

    #[test]
    fn submit_enforces_static_choices() {
        let task = task_with(vec![TaskParam {
            kind: ParamKind::Choice,
            choices: vec!["a".into(), "b".into()],
            ..string_param("c")
        }]);
        let mut form = FormState::new("t", &task).unwrap();
        form.fields[0].value = "z".into();
        assert!(form.submit().unwrap_err().contains("one of"));

        form.fields[0].value = "a".into();
        assert_eq!(form.submit().unwrap().get("c").unwrap(), "a");
    }

    #[test]
    fn int_stepper_clamps_to_bounds() {
        let task = task_with(vec![TaskParam {
            kind: ParamKind::Int,
            validate: Some(ParamValidate {
                min: Some(0),
                max: Some(3),
            }),
            default: Some("2".into()),
            ..string_param("n")
        }]);
        let mut form = FormState::new("t", &task).unwrap();
        form.fields[0].step_int(10);
        assert_eq!(form.fields[0].value, "3");
        form.fields[0].step_int(-99);
        assert_eq!(form.fields[0].value, "0");
    }

    #[test]
    fn bool_toggle_round_trips() {
        let task = task_with(vec![TaskParam {
            kind: ParamKind::Bool,
            ..string_param("b")
        }]);
        let mut form = FormState::new("t", &task).unwrap();
        form.fields[0].toggle_bool();
        assert_eq!(form.fields[0].value, "true");
        form.fields[0].toggle_bool();
        assert_eq!(form.fields[0].value, "false");
    }

    #[test]
    fn apply_completions_drops_stale_replies() {
        let task = task_with(vec![TaskParam {
            completions: Some(crate::config::Completions {
                cmd: "ls".into(),
                args: vec![],
                parse: crate::config::CompletionParse::Lines,
                cache: None,
                timeout: None,
            }),
            ..string_param("x")
        }]);
        let mut form = FormState::new("t", &task).unwrap();
        let id1 = form.start_request("x");
        let id2 = form.start_request("x");
        // First reply is stale because id2 superseded it — dropped.
        form.apply_completions("x", id1, Ok(vec!["stale".into()]));
        assert!(matches!(form.fields[0].candidates, CandidateState::Loading));
        // Second reply matches the current id — applied.
        form.apply_completions("x", id2, Ok(vec!["fresh".into()]));
        match &form.fields[0].candidates {
            CandidateState::Loaded(v) => assert_eq!(v, &vec!["fresh".to_string()]),
            other => panic!("expected Loaded, got {other:?}"),
        }
    }

    #[test]
    fn apply_completions_failure_is_applied() {
        let task = task_with(vec![TaskParam {
            completions: Some(crate::config::Completions {
                cmd: "false".into(),
                args: vec![],
                parse: crate::config::CompletionParse::Lines,
                cache: None,
                timeout: None,
            }),
            ..string_param("x")
        }]);
        let mut form = FormState::new("t", &task).unwrap();
        let id = form.start_request("x");
        form.apply_completions(
            "x",
            id,
            Err(CompletionError {
                message: "exit 1".into(),
                log_path: Some(std::path::PathBuf::from("/tmp/log")),
            }),
        );
        match &form.fields[0].candidates {
            CandidateState::Failed { message, log_path } => {
                assert_eq!(message, "exit 1");
                assert_eq!(log_path.as_deref(), Some(std::path::Path::new("/tmp/log")));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn visible_candidates_filters_by_value() {
        let mut field = Field::from_param(&TaskParam {
            choices: vec!["alpha".into(), "beta".into(), "gamma".into()],
            ..string_param("x")
        });
        assert_eq!(field.visible_candidates().len(), 3);
        field.value = "a".into();
        let got = field.visible_candidates();
        // All three contain 'a' — the exact ordering depends on the fuzzy
        // scorer, so just check the set.
        let mut sorted = got;
        sorted.sort();
        assert_eq!(sorted, vec!["alpha", "beta", "gamma"]);
        field.value = "be".into();
        let got = field.visible_candidates();
        assert!(got.iter().any(|s| s == "beta"));
        assert!(!got.iter().any(|s| s == "gamma"));
    }

    #[test]
    fn visible_candidate_window_scrolls_with_highlight() {
        let mut field = Field::from_param(&TaskParam {
            choices: vec![
                "a0".into(),
                "a1".into(),
                "a2".into(),
                "a3".into(),
                "a4".into(),
                "a5".into(),
                "a6".into(),
            ],
            ..string_param("x")
        });
        field.candidate_highlight = 5;

        let got = field.visible_candidate_window(5);
        assert_eq!(got.items, vec!["a1", "a2", "a3", "a4", "a5"]);
        assert_eq!(got.highlight, 4);
        assert_eq!(got.hidden_above, 1);
        assert_eq!(got.hidden_below, 1);
    }

    #[test]
    fn accept_highlighted_candidate_uses_full_filtered_list() {
        let mut field = Field::from_param(&TaskParam {
            choices: vec![
                "alpha".into(),
                "beta".into(),
                "gamma".into(),
                "delta".into(),
                "zeta".into(),
                "theta".into(),
            ],
            ..string_param("x")
        });
        field.candidate_highlight = 5;

        field.accept_highlighted_candidate();

        assert_eq!(field.value, "theta");
    }
}
