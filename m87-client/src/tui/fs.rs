// use crate::util::{
//     fs::{DirEntry, FileInfo, FsResponse},
//     logging::human_date,
// };

// fn blue(s: &str) -> String {
//     format!("\x1b[34m{}\x1b[0m", s)
// }
// fn green(s: &str) -> String {
//     format!("\x1b[32m{}\x1b[0m", s)
// }
// fn cyan(s: &str) -> String {
//     format!("\x1b[36m{}\x1b[0m", s)
// }
// fn red(s: &str) -> String {
//     format!("\x1b[31m{}\x1b[0m", s)
// }
// fn dim(s: &str) -> String {
//     format!("\x1b[2m{}\x1b[0m", s)
// }

// fn human_size(size: u64) -> String {
//     const KB: f64 = 1024.0;
//     const MB: f64 = KB * 1024.0;
//     const GB: f64 = MB * 1024.0;

//     if size < 1024 {
//         format!("{}B", size)
//     } else if size < (1024 * 1024) {
//         format!("{:.1}K", size as f64 / KB)
//     } else if size < (1024 * 1024 * 1024) {
//         format!("{:.1}M", size as f64 / MB)
//     } else {
//         format!("{:.1}G", size as f64 / GB)
//     }
// }

// pub fn print_fs_response(resp: &FsResponse) {
//     match resp {
//         FsResponse::Ok => {
//             println!("{}", green("OK"));
//         }

//         FsResponse::Err { message } => {
//             println!("{} {}", red("ERROR:"), message);
//         }

//         FsResponse::Data { data, eof } => {
//             println!(
//                 "{} ({} bytes){}",
//                 cyan("DATA"),
//                 data.len(),
//                 if *eof { " [EOF]" } else { "" }
//             );
//         }

//         FsResponse::FileInfo { info } => {
//             print_fileinfo(info, ".");
//         }

//         FsResponse::DirEntries { entries } => {
//             if entries.is_empty() {
//                 println!("(empty)");
//                 return;
//             }

//             // Compute width for alignment
//             let max_size = entries.iter().map(|e| e.size).max().unwrap_or(1);
//             let width = human_size(max_size).len();

//             for e in entries {
//                 print_direntry(e, width);
//             }
//         }
//     }
// }

// fn print_direntry(e: &DirEntry, size_width: usize) {
//     let size_str = human_size(e.size);

//     // enforce minimum width for nicer alignment (like ls)
//     let padded_size = format!("{:>width$}", size_str, width = size_width.max(6));

//     let timestamp = human_date(e.modified);

//     let name = if e.is_dir {
//         blue(&e.name)
//     } else {
//         e.name.clone()
//     };

//     println!("{} {} {}", padded_size, timestamp, name);
// }

// fn print_fileinfo(info: &FileInfo, name: &str) {
//     let size_str = human_size(info.size);
//     let padded_size = format!("{:>6}", size_str);
//     let timestamp = human_date(info.modified);

//     let colored = if info.is_dir {
//         blue(name)
//     } else {
//         name.to_string()
//     };

//     println!("{} {} {}", padded_size, timestamp, colored);
// }
