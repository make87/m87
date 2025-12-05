use crate::{
    auth::AuthManager,
    config::Config,
    devices,
    streams::{quic::open_quic_io, stream_type::StreamType},
};
use anyhow::{Context, Result, anyhow};
use m87_shared::metrics::SystemMetrics;

use ratatui::Terminal;
use tokio::io::{AsyncBufReadExt, BufReader};

pub async fn run_metrics(device: &str) -> Result<()> {
    let config = Config::load()?;
    let host = config.get_server_hostname();

    let dev = devices::get_device_by_name(device).await?;
    let token = AuthManager::get_cli_token().await?;

    let stream_type = StreamType::Metrics {
        token: token.to_string(),
    };
    let (_, io) = open_quic_io(
        &host,
        &dev.short_id,
        stream_type,
        config.trust_invalid_server_cert,
    )
    .await
    .context("Failed to connect to RAW metrics stream")?;

    // Read line-delimited JSON
    let reader = BufReader::new(io);
    let mut lines = reader.lines();

    // Channel to feed UI
    let (tx, rx) = tokio::sync::mpsc::channel::<SystemMetrics>(32);

    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(parsed) = serde_json::from_str::<SystemMetrics>(&line) {
                let _ = tx.send(parsed).await;
            }
        }
    });

    ui_loop(rx).await.map_err(|e| anyhow!("{:?}", e))?;

    Ok(())
}

pub async fn ui_loop(
    mut rx: tokio::sync::mpsc::Receiver<SystemMetrics>,
) -> Result<(), Box<dyn std::error::Error>> {
    use ratatui::{
        backend::TermionBackend,
        layout::{Alignment, Constraint, Direction, Layout, Rect},
        style::{Color, Style},
        widgets::{Block, Borders, Gauge, Paragraph, Row, Sparkline, Table},
    };
    use std::cmp::min;
    use std::collections::VecDeque;
    use termion::{
        async_stdin, event::Key, input::TermRead, raw::IntoRawMode, screen::IntoAlternateScreen,
    };

    const HISTORY_LEN: usize = 200;

    fn push_hist(hist: &mut VecDeque<f64>, v: f64) {
        hist.push_back(v);
        if hist.len() > HISTORY_LEN {
            hist.pop_front();
        }
    }

    fn spark_data(hist: &VecDeque<f64>, width: u16, scale: f64) -> Vec<u64> {
        let w = width.max(1) as usize;
        hist.iter()
            .rev() // newest on the right
            .take(w) // only what fits
            .map(|v| (v * scale) as u64)
            .collect()
    }

    // Terminal init
    let stdout = std::io::stdout();
    let raw = stdout.into_raw_mode()?;
    let screen = raw.into_alternate_screen()?;
    let backend = TermionBackend::new(screen);
    let mut terminal = Terminal::new(backend)?;

    let mut keys = async_stdin().keys();

    // State
    let mut latest: Option<SystemMetrics> = None;

    let mut cpu_core_history: Vec<VecDeque<f64>> = Vec::new();

    let mut mem_history: VecDeque<f64> = VecDeque::with_capacity(HISTORY_LEN);
    let mut disk_history: VecDeque<f64> = VecDeque::with_capacity(HISTORY_LEN);

    let mut net_rx_history: VecDeque<f64> = VecDeque::with_capacity(HISTORY_LEN);
    let mut net_tx_history: VecDeque<f64> = VecDeque::with_capacity(HISTORY_LEN);

    let mut gpu_mem_history: VecDeque<f64> = VecDeque::with_capacity(HISTORY_LEN);
    let mut gpu_util_history: VecDeque<f64> = VecDeque::with_capacity(HISTORY_LEN);

    loop {
        // keyboard
        if let Some(Ok(key)) = keys.next() {
            match key {
                Key::Ctrl('c') | Key::Char('q') | Key::Esc => return Ok(()),
                _ => {}
            }
        }

        // new metrics
        if let Ok(Some(m)) =
            tokio::time::timeout(std::time::Duration::from_millis(20), rx.recv()).await
        {
            // resize per-core histories
            let cores = m.cpu.per_core.len();
            if cpu_core_history.len() < cores {
                for _ in cpu_core_history.len()..cores {
                    cpu_core_history.push(VecDeque::with_capacity(HISTORY_LEN));
                }
            } else if cpu_core_history.len() > cores {
                cpu_core_history.truncate(cores);
            }

            for (i, core) in m.cpu.per_core.iter().enumerate() {
                push_hist(&mut cpu_core_history[i], core.usage_percent as f64);
            }

            push_hist(&mut mem_history, m.memory.usage_percent as f64);
            push_hist(&mut disk_history, m.disk.usage_percent as f64);

            push_hist(&mut net_rx_history, m.network.rx_mbps as f64);
            push_hist(&mut net_tx_history, m.network.tx_mbps as f64);

            if let Some(g) = m.gpu.first() {
                let mem_pct = if g.memory_total_mb == 0 {
                    0.0
                } else {
                    (g.memory_used_mb as f64 / g.memory_total_mb as f64) * 100.0
                };
                push_hist(&mut gpu_mem_history, mem_pct);
                push_hist(&mut gpu_util_history, g.usage_percent as f64);
            } else {
                gpu_mem_history.clear();
                gpu_util_history.clear();
            }

            latest = Some(m);
        }

        terminal.draw(|f| {
            let size = f.area();
            if latest.is_none() {
                return;
            }
            let m = latest.as_ref().unwrap();

            // -------- root layout --------
            let root_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),      // header
                    Constraint::Percentage(45), // CPU + NET
                    Constraint::Percentage(52), // MEM + DISK + GPU
                ])
                .split(size);

            let header_area = root_chunks[0];
            let cpu_net_area = root_chunks[1];
            let mem_disk_gpu_area = root_chunks[2];

            // -------- header --------
            let uptime_h = m.uptime_secs / 3600;
            let uptime_m = (m.uptime_secs % 3600) / 60;
            let header_text = format!(
                "{} | {} | {} | uptime {:02}h{:02}m | CPU {:4.1}%",
                m.hostname, m.os, m.arch, uptime_h, uptime_m, m.cpu.usage_percent
            );

            let header_paragraph = Paragraph::new(header_text)
                .alignment(Alignment::Center)
                .block(Block::default().borders(Borders::ALL).title("System"));

            f.render_widget(header_paragraph, header_area);

            // -------- CPU + NET row --------
            let cpu_net_rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(60), // CPU cores (full width)
                    Constraint::Percentage(40), // NET (table + RX/TX)
                ])
                .split(cpu_net_area);

            let cpu_core_area = cpu_net_rows[0];
            let net_area = cpu_net_rows[1];

            // ===== CPU CORE SPARKLINES (full width) =====
            {
                let cores = m.cpu.per_core.len();
                let max_per_col = 20;
                let num_cols = ((cores + max_per_col - 1) / max_per_col).max(1);

                let mut col_constraints = Vec::new();
                for _ in 0..num_cols {
                    col_constraints.push(Constraint::Percentage((100 / num_cols) as u16));
                }

                let cpu_core_cols = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints(col_constraints)
                    .split(cpu_core_area);

                for col_idx in 0..num_cols {
                    let start = col_idx * max_per_col;
                    if start >= cores {
                        break;
                    }
                    let end = min(start + max_per_col, cores);
                    let count = end - start;

                    let rows = ((count + 1) / 2).max(1);
                    let mut row_constraints = Vec::new();
                    for _ in 0..rows {
                        row_constraints.push(Constraint::Percentage((100 / rows) as u16));
                    }

                    let col_rect = cpu_core_cols[col_idx];
                    let row_rects = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints(row_constraints)
                        .split(col_rect);

                    for row_idx in 0..rows {
                        let core0_idx = start + row_idx * 2;
                        let core1_idx = core0_idx + 1;

                        let row_area = row_rects[row_idx];
                        let core_cols = Layout::default()
                            .direction(Direction::Horizontal)
                            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                            .split(row_area);

                        for (slot, core_idx) in [core0_idx, core1_idx].into_iter().enumerate() {
                            if core_idx >= end {
                                continue;
                            }
                            let area: Rect = core_cols[slot];
                            let dflt = VecDeque::new();
                            let hist = cpu_core_history.get(core_idx).unwrap_or(&dflt);

                            let data = spark_data(hist, area.width, 1.0);
                            let last = hist.back().cloned().unwrap_or(0.0);

                            let color = if last > 80.0 {
                                Color::Red
                            } else if last > 40.0 {
                                Color::Yellow
                            } else {
                                Color::Green
                            };

                            let spark = Sparkline::default()
                                .block(
                                    Block::default()
                                        .borders(Borders::ALL)
                                        .title(format!("core {}", core_idx)),
                                )
                                .data(&data)
                                .style(Style::default().fg(color));

                            f.render_widget(spark, area);

                            let label = Paragraph::new(format!("{:>4.0}%", last))
                                .alignment(Alignment::Center)
                                .style(Style::default().fg(Color::White));
                            f.render_widget(label, area);
                        }
                    }
                }
            }

            // ===== NETWORK (table left, RX/TX sparklines right) =====
            {
                let net_cols = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(net_area);

                let nic_area = net_cols[0];
                let net_hist_area = net_cols[1];

                // NIC table
                let mut ifaces = m.network.interfaces.clone();
                ifaces.sort_by(|a, b| a.name.cmp(&b.name));

                let rows: Vec<Row> = ifaces
                    .iter()
                    .map(|iface| {
                        Row::new(vec![
                            iface.name.clone(),
                            format!("{}", iface.rx_bytes),
                            format!("{}", iface.tx_bytes),
                        ])
                    })
                    .collect();

                let table = Table::new(
                    rows,
                    [
                        Constraint::Percentage(40),
                        Constraint::Percentage(30),
                        Constraint::Percentage(30),
                    ],
                )
                .header(
                    Row::new(vec!["Interface", "RX bytes", "TX bytes"])
                        .style(Style::default().fg(Color::LightBlue)),
                )
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Network Interfaces"),
                );

                f.render_widget(table, nic_area);

                // RX/TX sparklines
                let net_rows = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(net_hist_area);

                let rx_area = net_rows[0];
                let tx_area = net_rows[1];

                let rx_data = spark_data(&net_rx_history, rx_area.width, 1000.0);
                let tx_data = spark_data(&net_tx_history, tx_area.width, 1000.0);

                let rx_last = net_rx_history.back().cloned().unwrap_or(0.0);
                let tx_last = net_tx_history.back().cloned().unwrap_or(0.0);

                let rx_spark = Sparkline::default()
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Network RX (Mbps)"),
                    )
                    .data(&rx_data)
                    .style(Style::default().fg(Color::Blue));
                f.render_widget(rx_spark, rx_area);
                let rx_label = Paragraph::new(format!("{:.2} Mbps", rx_last))
                    .alignment(Alignment::Center)
                    .style(Style::default().fg(Color::White));
                f.render_widget(rx_label, rx_area);

                let tx_spark = Sparkline::default()
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Network TX (Mbps)"),
                    )
                    .data(&tx_data)
                    .style(Style::default().fg(Color::Cyan));
                f.render_widget(tx_spark, tx_area);
                let tx_label = Paragraph::new(format!("{:.2} Mbps", tx_last))
                    .alignment(Alignment::Center)
                    .style(Style::default().fg(Color::White));
                f.render_widget(tx_label, tx_area);
            }

            // -------- MEM + DISK + GPU row (2 cols, then 2 cols) --------
            let mdg_rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(50), // mem+disk
                    Constraint::Percentage(50), // gpu
                ])
                .split(mem_disk_gpu_area);

            let mem_disk_row = mdg_rows[0];
            let gpu_row = mdg_rows[1];

            let mem_disk_cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(mem_disk_row);

            let mem_area = mem_disk_cols[0];
            let disk_area = mem_disk_cols[1];

            // memory gauge + sparkline
            {
                let mem_rows = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Length(3), Constraint::Min(0)])
                    .split(mem_area);

                let gauge_area = mem_rows[0];
                let spark_area = mem_rows[1];

                let mem_gauge = Gauge::default()
                    .block(Block::default().borders(Borders::ALL).title("Memory"))
                    .gauge_style(Style::default().fg(Color::Cyan))
                    .ratio(m.memory.usage_percent as f64 / 100.0);
                f.render_widget(mem_gauge, gauge_area);

                let data = spark_data(&mem_history, spark_area.width, 1.0);
                let last = mem_history.back().cloned().unwrap_or(0.0);
                let spark = Sparkline::default()
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Memory Usage History (%)"),
                    )
                    .data(&data)
                    .style(Style::default().fg(Color::LightCyan));
                f.render_widget(spark, spark_area);
                let label = Paragraph::new(format!("{:4.1}%", last))
                    .alignment(Alignment::Center)
                    .style(Style::default().fg(Color::White));
                f.render_widget(label, spark_area);
            }

            // disk gauge + sparkline
            {
                let disk_rows = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Length(3), Constraint::Min(0)])
                    .split(disk_area);

                let gauge_area = disk_rows[0];
                let spark_area = disk_rows[1];

                let disk_gauge = Gauge::default()
                    .block(Block::default().borders(Borders::ALL).title("Disk"))
                    .gauge_style(Style::default().fg(Color::Magenta))
                    .ratio(m.disk.usage_percent as f64 / 100.0);
                f.render_widget(disk_gauge, gauge_area);

                let data = spark_data(&disk_history, spark_area.width, 1.0);
                let last = disk_history.back().cloned().unwrap_or(0.0);
                let spark = Sparkline::default()
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Disk Usage History (%)"),
                    )
                    .data(&data)
                    .style(Style::default().fg(Color::Magenta));
                f.render_widget(spark, spark_area);
                let label = Paragraph::new(format!("{:4.1}%", last))
                    .alignment(Alignment::Center)
                    .style(Style::default().fg(Color::White));
                f.render_widget(label, spark_area);
            }

            // GPU row
            if m.gpu.is_empty() {
                let block = Block::default().borders(Borders::ALL).title("GPU");
                let text = Paragraph::new("No GPU detected")
                    .block(block)
                    .alignment(Alignment::Center)
                    .style(Style::default().fg(Color::Gray));
                f.render_widget(text, gpu_row);
            } else {
                let g = &m.gpu[0];
                let gpu_cols = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(gpu_row);

                let gpu_mem_area = gpu_cols[0];
                let gpu_util_area = gpu_cols[1];

                // memory %
                let mem_last = gpu_mem_history.back().cloned().unwrap_or_else(|| {
                    if g.memory_total_mb == 0 {
                        0.0
                    } else {
                        (g.memory_used_mb as f64 / g.memory_total_mb as f64) * 100.0
                    }
                });
                let mem_data = spark_data(&gpu_mem_history, gpu_mem_area.width, 1.0);
                let mem_spark = Sparkline::default()
                    .block(Block::default().borders(Borders::ALL).title(format!(
                        "GPU {} Memory Usage (%) ({} / {} MB)",
                        g.name, g.memory_used_mb, g.memory_total_mb
                    )))
                    .data(&mem_data)
                    .style(Style::default().fg(Color::Green));
                f.render_widget(mem_spark, gpu_mem_area);
                let mem_label = Paragraph::new(format!("{:4.1}%", mem_last))
                    .alignment(Alignment::Center)
                    .style(Style::default().fg(Color::White));
                f.render_widget(mem_label, gpu_mem_area);

                // utilization %
                let util_last = gpu_util_history
                    .back()
                    .cloned()
                    .unwrap_or(g.usage_percent as f64);
                let util_data = spark_data(&gpu_util_history, gpu_util_area.width, 1.0);
                let util_spark = Sparkline::default()
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("GPU Utilization (%)"),
                    )
                    .data(&util_data)
                    .style(Style::default().fg(Color::Yellow));
                f.render_widget(util_spark, gpu_util_area);
                let util_label = Paragraph::new(format!("{:4.1}%", util_last))
                    .alignment(Alignment::Center)
                    .style(Style::default().fg(Color::White));
                f.render_widget(util_label, gpu_util_area);
            }
        })?;
    }
}
