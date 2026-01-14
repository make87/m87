use std::io::{self, Write};

use tokio::{
    sync::broadcast,
    time::{Duration, interval},
};

const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

const SUCCESS_COLOR: &str = "\x1b[32m";
const ERROR_COLOR: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

#[derive(Debug, Clone)]
pub enum UiEvent {
    LoadingStart,
    Line(String),
    Done { ok: bool, text: String },
}

impl UiEvent {
    /// Convert to a plain log line if it makes sense to forward
    pub fn as_line(&self) -> Option<String> {
        match self {
            UiEvent::Line(line) => Some(line.clone()),

            UiEvent::Done { ok, text } => {
                let symbol = if *ok { "✓" } else { "✗" };
                Some(format!("{symbol} {text}"))
            }

            // LoadingStart is a UI concern only
            UiEvent::LoadingStart => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as_line_from_line() {
        let event = UiEvent::Line("hello world".to_string());
        assert_eq!(event.as_line(), Some("hello world".to_string()));
    }

    #[test]
    fn test_as_line_from_done_ok() {
        let event = UiEvent::Done {
            ok: true,
            text: "success".to_string(),
        };
        assert_eq!(event.as_line(), Some("✓ success".to_string()));
    }

    #[test]
    fn test_as_line_from_done_error() {
        let event = UiEvent::Done {
            ok: false,
            text: "failed".to_string(),
        };
        assert_eq!(event.as_line(), Some("✗ failed".to_string()));
    }

    #[test]
    fn test_as_line_from_loading_start() {
        let event = UiEvent::LoadingStart;
        assert_eq!(event.as_line(), None);
    }
}

pub async fn run_renderer(mut rx: broadcast::Receiver<UiEvent>) {
    let mut loading = false;
    let mut current = String::new();
    let mut frame = 0usize;

    let mut tick = interval(Duration::from_millis(80));

    loop {
        tokio::select! {
            Ok(ev) = rx.recv() => {
                match ev {
                    UiEvent::LoadingStart => {
                        loading = true;
                        current.clear();
                        frame = 0;
                    }

                    UiEvent::Line(line) => {
                        if loading {
                            current = line;
                        } else {
                            println!("{line}");
                        }
                    }

                    UiEvent::Done { ok, text } => {
                        if loading {
                            print!("\r\x1b[2K");
                            let symbol = if ok { "✓" } else { "✗" };
                            let color = if ok { SUCCESS_COLOR } else { ERROR_COLOR };
                            println!("{color}{symbol} {text}{RESET}");
                            loading = false;
                            current.clear();
                        } else {
                            println!("{text}");
                        }
                    }
                }

                let _ = io::stdout().flush();
            }

            _ = tick.tick(), if loading => {
                let s = SPINNER[frame % SPINNER.len()];
                frame += 1;

                print!("\r{s} {current}");
                let _ = io::stdout().flush();
            }
        }
    }
}
