use chrono::{SecondsFormat, Utc};

const RESET: &str = "\x1b[0m";
const GREY: &str = "\x1b[90m";
const CYAN: &str = "\x1b[36m";
const WHITE: &str = "\x1b[37m";

pub fn format_log(source: &str, message: &str, ansi: bool) -> String {
    let ts = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
    let msg = message.trim_end_matches('\n');

    if !ansi {
        return format!("[{ts}] [{source}] {msg}");
    }

    format!(
        "{grey}[{ts}]{reset} {cyan}[{source}]{reset} {white}{msg}{reset}",
        grey = GREY,
        cyan = CYAN,
        white = WHITE,
        reset = RESET,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_log_without_ansi() {
        let result = format_log("myapp", "Hello world", false);
        // Format: "2024-01-01T12:00:00.000000Z myapp: Hello world"
        assert!(result.contains("myapp"));
        assert!(result.contains("Hello world"));
        assert!(result.contains("T")); // ISO timestamp has T separator
        assert!(result.contains("Z")); // UTC timezone marker
        // Should not contain ANSI codes
        assert!(!result.contains("\x1b["));
    }

    #[test]
    fn test_format_log_with_ansi() {
        let result = format_log("myapp", "Hello world", true);
        // Should contain ANSI escape codes
        assert!(result.contains(GREY));
        assert!(result.contains(CYAN));
        assert!(result.contains(WHITE));
        assert!(result.contains(RESET));
        // Should still contain the content
        assert!(result.contains("myapp"));
        assert!(result.contains("Hello world"));
    }

    #[test]
    fn test_format_log_trims_trailing_newline() {
        let result = format_log("src", "message\n", false);
        assert!(result.ends_with("message"));
        assert!(!result.ends_with("message\n"));

        // Multiple newlines - only trailing ones trimmed
        let result = format_log("src", "line1\nline2\n", false);
        assert!(result.contains("line1\nline2"));
        assert!(!result.ends_with('\n'));
    }

    #[test]
    fn test_format_log_empty_message() {
        let result = format_log("src", "", false);
        assert!(result.contains("src"));
    }

    #[test]
    fn test_format_log_empty_source() {
        let result = format_log("", "message", false);
        assert!(result.contains(" message"));
    }
}
