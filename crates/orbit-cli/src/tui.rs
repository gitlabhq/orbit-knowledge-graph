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

#[cfg(unix)]
static TERMINAL_BEFORE_ECHO_OFF: std::sync::Mutex<Option<rustix::termios::Termios>> =
    std::sync::Mutex::new(None);

/// The terminal echoes Ctrl-C as `^C`, which shifts the progress bars cliclack redraws in place.
pub(crate) struct ControlEchoOff;

pub(crate) fn turn_off_control_echo() -> ControlEchoOff {
    #[cfg(unix)]
    if let Ok(saved) = rustix::termios::tcgetattr(std::io::stdin()) {
        let mut quiet = saved.clone();
        quiet
            .local_modes
            .remove(rustix::termios::LocalModes::ECHOCTL);
        let _ = rustix::termios::tcsetattr(
            std::io::stdin(),
            rustix::termios::OptionalActions::Now,
            &quiet,
        );
        if let Ok(mut before) = TERMINAL_BEFORE_ECHO_OFF.lock() {
            *before = Some(saved);
        }
    }
    ControlEchoOff
}

pub(crate) fn restore_control_echo() {
    #[cfg(unix)]
    if let Some(saved) = TERMINAL_BEFORE_ECHO_OFF
        .lock()
        .ok()
        .and_then(|mut before| before.take())
    {
        let _ = rustix::termios::tcsetattr(
            std::io::stdin(),
            rustix::termios::OptionalActions::Now,
            &saved,
        );
    }
}

impl Drop for ControlEchoOff {
    fn drop(&mut self) {
        restore_control_echo();
    }
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
        Bar {
            bar,
            label: label.to_string(),
        }
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

    pub(crate) fn cancel(&self) {
        self.0.cancel();
    }
}

pub(crate) struct Bar {
    bar: cliclack::ProgressBar,
    label: String,
}

impl Bar {
    pub(crate) fn advance(&self, count: usize) {
        self.bar.inc(count as u64);
    }

    pub(crate) fn finish(&self, message: impl Display) {
        self.bar.stop(message);
    }

    pub(crate) fn cancel_at_current_count(&self) {
        self.bar.cancel(self.label_with_current_count());
    }

    pub(crate) fn fail_at_current_count(&self) {
        self.bar.error(self.label_with_current_count());
    }

    fn label_with_current_count(&self) -> String {
        format!(
            "{}  {}/{}",
            self.label,
            format_with_thousands(self.bar.position() as usize),
            format_with_thousands(self.bar.length().unwrap_or_default() as usize)
        )
    }
}

pub(crate) fn cancelled(message: &str) -> anyhow::Error {
    std::io::Error::new(std::io::ErrorKind::Interrupted, message.to_string()).into()
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
    cliclack::set_theme(KeyHintsFooter);
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
struct KeyHintsFooter;

impl Theme for KeyHintsFooter {
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
