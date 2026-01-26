const RESET: &str = "\x1b[0m";
const CYAN: &str = "\x1b[36m";
const WHITE: &str = "\x1b[37m";

pub fn format_log(source: &str, message: &str, ansi: bool) -> String {
    // let ts = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
    let msg = message.trim_end_matches('\n');

    if !ansi {
        return format!("{source}: {msg}");
    }

    format!(
        "{cyan}{source}{reset}: {white}{msg}{reset}",
        cyan = CYAN,
        white = WHITE,
        reset = RESET,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_format_log_with_ansi() {
        let result = format_log("myapp", "Hello world", true);
        // Should contain ANSI escape codes
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
        assert!(result.contains("src: "));
    }

    #[test]
    fn test_format_log_empty_source() {
        let result = format_log("", "message", false);
        assert!(result.contains(": message"));
    }
}
