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

    cliclack::set_theme(KeyHintsFooter);
    let chosen = picker.interact();
    cliclack::reset_theme();
    Ok(chosen?.into_iter().map(str::to_string).collect())
}

struct KeyHintsFooter;

impl Theme for KeyHintsFooter {
    fn format_footer_with_message(&self, state: &ThemeState, message: &str) -> String {
        struct Stock;
        impl Theme for Stock {}

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
