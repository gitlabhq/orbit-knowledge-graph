//! Terminal widgets shared by interactive commands. The only module that imports cliclack.

use std::fmt::Display;
use std::io::IsTerminal;

use anyhow::{Result, bail};
use cliclack::{Theme, ThemeState};

pub(crate) struct Choice {
    pub(crate) key: String,
    pub(crate) label: String,
    pub(crate) hint: String,
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
    cliclack::set_theme(OrbitTheme);
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

pub(crate) fn warn(message: impl Display) {
    let _ = cliclack::log::warning(message);
}

pub(crate) fn error(message: impl Display) {
    let _ = cliclack::log::error(message);
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

pub(crate) struct ProgressGroup(cliclack::MultiProgress);

pub(crate) fn progress_group(title: impl Display) -> ProgressGroup {
    ProgressGroup(cliclack::multi_progress(title))
}

impl ProgressGroup {
    pub(crate) fn bar(&self, label: &str, total: usize) -> Bar {
        let bar = self.0.add(
            cliclack::progress_bar(total as u64)
                .with_template("{msg} {bar:30.magenta} {human_pos}/{human_len}"),
        );
        bar.start(label);
        Bar(bar)
    }

    pub(crate) fn note(&self, line: impl Display) {
        self.0.println(line);
    }

    pub(crate) fn close(&self) {
        self.0.stop();
    }

    pub(crate) fn fail(&self, message: impl Display) {
        self.0.error(message);
    }
}

pub(crate) struct Bar(cliclack::ProgressBar);

impl Bar {
    pub(crate) fn advance(&self, count: usize) {
        self.0.inc(count as u64);
    }

    pub(crate) fn finish(&self, message: impl Display) {
        self.0.stop(message);
    }
}

pub(crate) struct Spinner(cliclack::ProgressBar);

pub(crate) fn spinner(label: impl Display) -> Spinner {
    let bar = cliclack::spinner();
    bar.start(label);
    Spinner(bar)
}

impl Spinner {
    pub(crate) fn stop(self, message: impl Display) {
        self.0.stop(message);
    }

    pub(crate) fn error(self, message: impl Display) {
        self.0.error(message);
    }
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
    Ok(picker.interact()?.into_iter().map(str::to_string).collect())
}

struct Stock;

impl Theme for Stock {}

/// Stock theme plus key hints under pickers and cards without the empty top row.
struct OrbitTheme;

impl Theme for OrbitTheme {
    fn format_note(&self, prompt: &str, message: &str) -> String {
        let card = Stock.format_note(prompt, message);
        let mut lines: Vec<&str> = card.lines().collect();
        if let Some(box_top) = lines.iter().position(|line| line.contains('╮'))
            && box_top + 1 < lines.len()
        {
            lines.remove(box_top + 1);
        }
        lines.join("\n") + "\n"
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
