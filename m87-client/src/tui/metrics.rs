use crate::{auth::AuthManager, config::Config, devices, util::raw_connection::open_raw_io};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use ratatui::Terminal;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Deserialize, Debug)]
pub struct CpuInfo {
    pub name: String,
    pub usage_percent: f32,
}

#[derive(Deserialize, Debug)]
pub struct MemoryInfo {
    pub total: u64,
    pub used: u64,
    pub free: u64,
    pub usage_percent: f64,
    pub total_swap: u64,
    pub used_swap: u64,
}

#[derive(Deserialize, Debug)]
pub struct NetInfo {
    pub name: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

#[derive(Deserialize, Debug)]
pub struct DiskInfo {
    pub name: String,
    pub total_space: u64,
    pub available_space: u64,
}

#[derive(Deserialize, Debug)]
pub struct TempInfo {
    pub component: String,
    pub temperature: f32,
}

#[derive(Deserialize, Debug)]
pub struct Metrics {
    pub cpus: Vec<CpuInfo>,
    pub memory: MemoryInfo,
    pub networks: Vec<NetInfo>,
    pub disks: Vec<DiskInfo>,
    pub temperatures: Vec<TempInfo>,
    pub time: u64,
}

pub async fn run_metrics(device: &str) -> Result<()> {
    let config = Config::load()?;
    let host = config.get_server_hostname();

    let dev = devices::get_device_by_name(device).await?;

    let token = AuthManager::get_cli_token().await?;

    let io = open_raw_io(
        &host,
        &dev.short_id,
        "/metrics",
        &token,
        config.trust_invalid_server_cert,
    )
    .await
    .context("Failed to connect to RAW metrics stream")?;

    // We'll read line-delimited JSON
    let reader = BufReader::new(io);
    let mut lines = reader.lines();

    // Channel to feed UI
    let (tx, rx) = tokio::sync::mpsc::channel::<Metrics>(32);

    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(parsed) = serde_json::from_str::<Metrics>(&line) {
                let _ = tx.send(parsed).await;
            }
        }
    });

    ui_loop(rx).await.map_err(|e| anyhow!("{:?}", e))?;

    Ok(())
}

pub async fn ui_loop(
    mut rx: tokio::sync::mpsc::Receiver<Metrics>,
) -> Result<(), Box<dyn std::error::Error>> {
    use ratatui::{
        backend::TermionBackend,
        layout::{Constraint, Direction, Layout},
        style::{Color, Style},
        text::Line,
        widgets::{Axis, Block, Borders, Chart, Dataset, Gauge, GraphType, Paragraph, Row, Table},
    };
    use std::collections::VecDeque;
    use termion::{
        async_stdin, event::Key, input::TermRead, raw::IntoRawMode, screen::IntoAlternateScreen,
    };

    // Terminal init
    let stdout = std::io::stdout();
    let raw = stdout.into_raw_mode()?; // enter raw mode
    let screen = raw.into_alternate_screen()?; // switch to alt screen
    let backend = TermionBackend::new(screen);
    let mut terminal = Terminal::new(backend)?;

    // Non-blocking key poll
    let mut keys = async_stdin().keys();

    // State
    let mut latest: Option<Metrics> = None;
    let mut cpu_history: VecDeque<f64> = VecDeque::with_capacity(200);

    loop {
        // -------------------------------------------------------------
        // Keyboard input
        // -------------------------------------------------------------
        if let Some(Ok(key)) = keys.next() {
            match key {
                Key::Ctrl('c') | Key::Char('q') | Key::Esc => {
                    return Ok(());
                }
                _ => {}
            }
        }

        // -------------------------------------------------------------
        // Metrics from websocket
        // -------------------------------------------------------------
        if let Ok(Some(m)) =
            tokio::time::timeout(std::time::Duration::from_millis(5), rx.recv()).await
        {
            latest = Some(m);
        }

        // -------------------------------------------------------------
        // UI Rendering
        // -------------------------------------------------------------
        terminal.draw(|f| {
            let size = f.size();

            // TOP-LEVEL: 2 columns
            let main_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(size);

            let left = main_chunks[0];
            let right = main_chunks[1];

            // LEFT COLUMN
            let left_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(4),  // memory
                    Constraint::Length(10), // CPU list
                    Constraint::Min(5),     // network
                ])
                .split(left);

            // RIGHT COLUMN
            let right_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(10), // CPU graph
                    Constraint::Length(10), // disks
                    Constraint::Min(5),     // temps
                ])
                .split(right);

            // Nothing yet
            if latest.is_none() {
                return;
            }

            let m = latest.as_ref().unwrap();

            // =====================================================================
            // MEMORY GAUGE
            // =====================================================================
            let mem = Gauge::default()
                .block(Block::default().title("Memory").borders(Borders::ALL))
                .gauge_style(Style::default().fg(Color::Cyan))
                .ratio(m.memory.usage_percent / 100.0);

            f.render_widget(mem, left_chunks[0]);

            // =====================================================================
            // CPU LIST
            // =====================================================================
            let cpu_lines: Vec<Line> = m
                .cpus
                .iter()
                .map(|cpu| {
                    let color = if cpu.usage_percent > 80.0 {
                        Color::Red
                    } else if cpu.usage_percent > 40.0 {
                        Color::Yellow
                    } else {
                        Color::Green
                    };

                    Line::styled(
                        format!("{:<10} {:>5.1}%", cpu.name, cpu.usage_percent),
                        Style::default().fg(color),
                    )
                })
                .collect();

            let cpu_widget = Paragraph::new(cpu_lines)
                .block(Block::default().title("CPU Usage").borders(Borders::ALL));

            f.render_widget(cpu_widget, left_chunks[1]);

            // =====================================================================
            // NETWORK TABLE
            // =====================================================================
            let net_rows: Vec<Row> = m
                .networks
                .iter()
                .map(|n| {
                    Row::new(vec![
                        n.name.clone(),
                        format!("{}", n.rx_bytes),
                        format!("{}", n.tx_bytes),
                    ])
                })
                .collect();

            let net_table = Table::new(
                net_rows,
                [
                    Constraint::Percentage(30),
                    Constraint::Percentage(35),
                    Constraint::Percentage(35),
                ],
            )
            .header(
                Row::new(vec!["Interface", "RX bytes", "TX bytes"])
                    .style(Style::default().fg(Color::LightBlue)),
            )
            .block(Block::default().title("Network").borders(Borders::ALL));

            f.render_widget(net_table, left_chunks[2]);

            // =====================================================================
            // CPU HISTORY GRAPH
            // =====================================================================
            // Compute avg CPU load for the graph
            let avg_cpu = m.cpus.iter().map(|c| c.usage_percent as f64).sum::<f64>()
                / m.cpus.len().max(1) as f64;

            cpu_history.push_back(avg_cpu);
            if cpu_history.len() > 200 {
                cpu_history.pop_front();
            }

            let points: Vec<(f64, f64)> = cpu_history
                .iter()
                .enumerate()
                .map(|(i, v)| (i as f64, *v))
                .collect();

            let cpu_chart = Chart::new(vec![Dataset::default()
                .name("CPU %")
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::LightCyan))
                .data(&points)])
            .block(Block::default().title("CPU History").borders(Borders::ALL))
            .x_axis(Axis::default().bounds([0.0, points.len().max(1) as f64]))
            .y_axis(Axis::default().bounds([0.0, 100.0]));

            f.render_widget(cpu_chart, right_chunks[0]);

            // =====================================================================
            // DISK TABLE
            // =====================================================================
            let disk_rows: Vec<Row> = m
                .disks
                .iter()
                .map(|d| {
                    let used = d.total_space.saturating_sub(d.available_space);
                    let pct = if d.total_space > 0 {
                        (used as f64 / d.total_space as f64) * 100.0
                    } else {
                        0.0
                    };

                    Row::new(vec![
                        d.name.clone(),
                        format!("{:.1}%", pct),
                        format!("{}", d.total_space),
                        format!("{}", d.available_space),
                    ])
                })
                .collect();

            let disk_table = Table::new(
                disk_rows,
                [
                    Constraint::Percentage(25),
                    Constraint::Percentage(15),
                    Constraint::Percentage(30),
                    Constraint::Percentage(30),
                ],
            )
            .header(
                Row::new(vec!["Disk", "Used %", "Total", "Free"])
                    .style(Style::default().fg(Color::Magenta)),
            )
            .block(Block::default().title("Disks").borders(Borders::ALL));

            f.render_widget(disk_table, right_chunks[1]);

            // =====================================================================
            // TEMPERATURES
            // =====================================================================
            let temp_lines: Vec<Line> = m
                .temperatures
                .iter()
                .map(|t| {
                    let color = if t.temperature > 80.0 {
                        Color::Red
                    } else if t.temperature > 60.0 {
                        Color::Yellow
                    } else {
                        Color::Green
                    };

                    Line::styled(
                        format!("{:<20} {:>5.1}°C", t.component, t.temperature),
                        Style::default().fg(color),
                    )
                })
                .collect();

            let temps = Paragraph::new(temp_lines)
                .block(Block::default().title("Temperatures").borders(Borders::ALL));

            f.render_widget(temps, right_chunks[2]);
        })?;
    }
}
