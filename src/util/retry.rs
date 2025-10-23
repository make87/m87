#[macro_export]
macro_rules! retry {
    ($times:expr, $delay:expr, $func:expr) => {{
        let mut attempts = 0;
        let mut delay = $delay;

        let result = loop {
            attempts += 1;
            let res = $func;

            if res.is_ok() || attempts >= $times {
                break res;
            } else {
                std::thread::sleep(std::time::Duration::from_millis(delay));
                delay *= 2; // Exponential backoff
            }
        };

        result
    }};
}

#[macro_export]
macro_rules! retry_async {
    ($times:expr, $delay:expr, $func:expr) => {{
        let mut attempts = 0;
        let mut delay = $delay;

        let result = loop {
            attempts += 1;
            let res = $func.await;

            if res.is_ok() || attempts >= $times {
                break res;
            } else {
                std::thread::sleep(std::time::Duration::from_millis(delay));
                delay *= 2; // Exponential backoff
            }
        };

        result
    }};
}
