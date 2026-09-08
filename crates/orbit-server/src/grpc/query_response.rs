use std::time::Duration;

use prost::Message;
use query_engine::formatters::{FormatName, GoonFormatter, GraphFormatter, ResultFormatter};
use query_engine::pipeline::PipelineError;
use query_engine::shared::{PaginationMeta, PipelineOutput, pagination_for_rows};
use query_engine::types::QueryResultRow;

use crate::proto::{
    ExecuteQueryMessage, ExecuteQueryResult, FormatName as ProtoFormatName, QueryMetadata,
    ResponseFormat, execute_query_message, execute_query_result,
};

pub(crate) struct QueryResponseOptions {
    pub format: ResponseFormat,
    pub max_response_bytes: usize,
    pub timeout: Duration,
}

pub(crate) async fn build_query_response(
    output: &mut PipelineOutput,
    options: &QueryResponseOptions,
) -> Result<ExecuteQueryMessage, PipelineError> {
    let formatter: &dyn ResultFormatter = match options.format {
        ResponseFormat::Raw => &GraphFormatter,
        ResponseFormat::Llm => &GoonFormatter,
    };
    let rows = output.query_result.rows();
    let response = encode_query_response(output, rows, output.pagination.as_ref(), formatter);

    tokio::task::yield_now().await;

    let response_bytes = response.encoded_len();
    if response_bytes <= options.max_response_bytes {
        return Ok(response);
    }
    drop(response);

    if output.compiled.input.cursor.is_none() || rows.len() < 2 {
        return Err(PipelineError::ResultTooLarge);
    }

    let estimated_row_count = ((rows.len() as u128 * options.max_response_bytes as u128)
        / response_bytes as u128) as usize;
    let estimated_row_count = estimated_row_count.clamp(1, rows.len() - 1);

    let mut fitting_row_count = 0;
    let mut upper_row_bound = rows.len();
    let mut candidate_row_count = estimated_row_count;

    while fitting_row_count + 1 < upper_row_bound {
        let candidate_rows = &rows[..candidate_row_count];
        let response = encode_query_response(output, candidate_rows, None, formatter);

        tokio::task::yield_now().await;

        let response_fits = response.encoded_len() <= options.max_response_bytes;

        if response_fits {
            fitting_row_count = candidate_row_count;
        } else {
            upper_row_bound = candidate_row_count;
        }

        let is_initial_probe = candidate_row_count == estimated_row_count;

        candidate_row_count = if !is_initial_probe {
            fitting_row_count + (upper_row_bound - fitting_row_count) / 2
        } else if response_fits {
            candidate_row_count + 1
        } else {
            candidate_row_count - 1
        };
    }

    for candidate_count in (1..=fitting_row_count).rev() {
        let candidate_rows = &rows[..candidate_count];
        let pagination = pagination_for_rows(candidate_rows, &output.compiled.input, true);
        let response = encode_query_response(output, candidate_rows, Some(&pagination), formatter);

        tokio::task::yield_now().await;

        if response.encoded_len() > options.max_response_bytes {
            continue;
        }

        let first_omitted = pagination_for_rows(
            &rows[candidate_count..candidate_count + 1],
            &output.compiled.input,
            true,
        );
        if pagination.next_cursor.is_none() || first_omitted.next_cursor.is_none() {
            return Err(PipelineError::Execution(
                "Cannot construct a lossless continuation cursor for the response page".into(),
            ));
        }
        if pagination.next_cursor == first_omitted.next_cursor {
            continue;
        }

        output.row_count = candidate_rows
            .iter()
            .filter(|row| row.is_authorized())
            .count();
        output.pagination = Some(pagination);
        output.query_result.truncate(candidate_count);

        return Ok(response);
    }

    Err(PipelineError::ResultTooLarge)
}

fn encode_query_response(
    output: &PipelineOutput,
    rows: &[QueryResultRow],
    pagination: Option<&PaginationMeta>,
    formatter: &dyn ResultFormatter,
) -> ExecuteQueryMessage {
    use execute_query_result::Content;

    let formatted = formatter.serialize_rows(output, rows, pagination);
    let (content, format_name) = match formatter.format_name() {
        FormatName::Raw => (Content::ResultJson(formatted), ProtoFormatName::Raw),
        FormatName::Goon => (Content::FormattedText(formatted), ProtoFormatName::Goon),
    };
    let row_count = rows.iter().filter(|row| row.is_authorized()).count();

    ExecuteQueryMessage {
        content: Some(execute_query_message::Content::Result(ExecuteQueryResult {
            content: Some(content),
            metadata: Some(QueryMetadata {
                query_type: output.query_type.clone(),
                raw_query_strings: output.raw_query_strings.clone(),
                row_count: i32::try_from(row_count).unwrap_or(i32::MAX),
                format_version: formatter
                    .format_version()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                format_name: format_name.into(),
            }),
        })),
    }
}
