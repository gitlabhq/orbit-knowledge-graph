//! Terminal widgets shared by interactive commands. The only module that imports cliclack.

use std::fmt::Display;
use std::io::IsTerminal;

use anyhow::{Result, bail};
use cliclack::{Theme, ThemeState};

pub(crate) struct Choice {
    pub(crate) key: String,
    pub(crate) label: String,
    pub(crate) hint: String,
    pub(crate) section: Option<String>,
}

pub(crate) fn can_prompt(skip_prompts: bool) -> Result<bool> {
    if skip_prompts {
        return Ok(false);
    }
    if !std::io::stdin().is_terminal() {
        bail!("stdin is not a terminal; pass --yes to proceed without a prompt");
    }
    Ok(true)
}

pub(crate) fn intro(title: impl Display) -> Result<()> {
    cliclack::set_theme(CompactCards);
    Ok(cliclack::intro(title)?)
}

pub(crate) fn outro(message: impl Display) -> Result<()> {
    Ok(cliclack::outro(message)?)
}

pub(crate) fn outro_cancel(message: impl Display) -> Result<()> {
    Ok(cliclack::outro_cancel(message)?)
}

pub(crate) fn card(title: impl Display, body: impl Display) -> Result<()> {
    Ok(cliclack::note(title, body)?)
}

pub(crate) fn warning(message: impl Display) {
    let _ = cliclack::log::warning(message);
}

pub(crate) fn error(message: impl Display) {
    let _ = cliclack::log::error(message);
}

pub(crate) fn align_columns<'a>(rows: impl Iterator<Item = (&'a str, String)>) -> String {
    let rows: Vec<(&str, String)> = rows.collect();
    let width = rows
        .iter()
        .map(|(name, _)| name.chars().count())
        .max()
        .unwrap_or_default();
    rows.iter()
        .map(|(name, detail)| format!("{name:<width$}   {detail}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn format_with_thousands(count: usize) -> String {
    let digits = count.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

pub(crate) struct Spinner(cliclack::ProgressBar);

pub(crate) fn spinner(message: impl Display) -> Spinner {
    let spinner = cliclack::spinner();
    spinner.start(message);
    Spinner(spinner)
}

impl Spinner {
    pub(crate) fn clear(&self) {
        self.0.clear();
    }

    pub(crate) fn fail(&self, message: impl Display) {
        self.0.error(message);
    }
}

pub(crate) fn is_cancelled(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|io| io.kind() == std::io::ErrorKind::Interrupted)
}

pub(crate) fn multiselect(
    prompt: &str,
    choices: &[Choice],
    preselected: &[String],
) -> Result<Vec<String>> {
    let mut picker = cliclack::multiselect(prompt).required(false);
    for choice in choices {
        picker = picker.item(choice.key.as_str(), &choice.label, &choice.hint);
    }
    picker = picker.initial_values(preselected.iter().map(String::as_str).collect());
    let sections = choices
        .iter()
        .filter_map(|choice| Some((choice.label.clone(), choice.section.clone()?)))
        .collect();
    cliclack::set_theme(KeyHintsFooter { sections });
    let chosen = picker.interact();
    cliclack::set_theme(CompactCards);
    Ok(chosen?.into_iter().map(str::to_string).collect())
}

struct Stock;

impl Theme for Stock {}

/// Set for the whole run: cards without the empty top row.
struct CompactCards;

impl Theme for CompactCards {
    fn format_note(&self, prompt: &str, message: &str) -> String {
        let card = Stock.format_note(prompt, message);
        let mut lines: Vec<&str> = card.split_inclusive('\n').collect();
        if let Some(box_top) = lines.iter().position(|line| line.contains('╮'))
            && box_top + 1 < lines.len()
        {
            lines.remove(box_top + 1);
        }
        lines.concat()
    }
}

/// Set only while a picker is open: every active widget shares this footer.
struct KeyHintsFooter {
    sections: Vec<(String, String)>,
}

impl Theme for KeyHintsFooter {
    fn format_multiselect_item(
        &self,
        state: &ThemeState,
        selected: bool,
        active: bool,
        label: &str,
        hint: &str,
    ) -> String {
        let item = Stock.format_multiselect_item(state, selected, active, label, hint);
        let heading = self
            .sections
            .iter()
            .find(|(item_label, _)| item_label == label)
            .map(|(_, heading)| heading);
        match (state, heading) {
            (ThemeState::Active | ThemeState::Error(_), Some(heading)) => {
                let bar = self.bar_color(state).apply_to("│");
                let heading = self.placeholder_style(state).apply_to(heading);
                format!("{bar}\n{bar}  {heading}\n{item}")
            }
            _ => item,
        }
    }

    fn format_footer_with_message(&self, state: &ThemeState, message: &str) -> String {
        let keys = match state {
            ThemeState::Active => "space toggles, enter confirms",
            _ => "",
        };
        let footer = [message, keys]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("  ");
        Stock.format_footer_with_message(state, &footer)
    }
}
