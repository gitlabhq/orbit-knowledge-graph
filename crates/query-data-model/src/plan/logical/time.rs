use chrono::{NaiveDate, NaiveDateTime};

pub(super) fn date(text: &str) -> bool {
    let text = text.strip_suffix('Z').unwrap_or(text);
    text.len() == 10 && calendar(text) && NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok()
}

pub(super) fn date_time(text: &str) -> bool {
    let bare = text.strip_suffix('Z').unwrap_or(text);
    date(text)
        || calendar(bare)
            && ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
                .iter()
                .any(|format| NaiveDateTime::parse_from_str(bare, format).is_ok())
}

fn calendar(text: &str) -> bool {
    text.get(..10).is_some_and(|date| {
        date.bytes().enumerate().all(|(index, byte)| match index {
            4 | 7 => byte == b'-',
            _ => byte.is_ascii_digit(),
        })
    })
}
