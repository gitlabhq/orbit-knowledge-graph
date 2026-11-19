use std::fmt;
use tracing::Event;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, format};

struct RustPrefixFormatter;

impl<S, N> FormatEvent<S, N> for RustPrefixFormatter
where
    S: for<'a> tracing_subscriber::registry::LookupSpan<'a> + tracing::Subscriber,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        write!(writer, "[RUST] ")?; // Add your prefix here
        format().format_event(ctx, writer, event)
    }
}

pub fn init_tracing(
    with_stderr: bool,
    with_colors: bool,
    with_target: bool,
    with_thread_ids: bool,
    with_thread_names: bool,
) {
    static INIT: std::sync::Once = std::sync::Once::new();
    if with_stderr {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_writer(std::io::stderr)
                .with_ansi(with_colors)
                .with_target(with_target)
                .with_thread_ids(with_thread_ids)
                .with_thread_names(with_thread_names)
                .event_format(RustPrefixFormatter)
                .init();
        });
    } else {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_writer(std::io::stdout)
                .with_ansi(with_colors)
                .with_target(with_target)
                .with_thread_ids(with_thread_ids)
                .with_thread_names(with_thread_names)
                .event_format(RustPrefixFormatter)
                .init();
        });
    }
}
