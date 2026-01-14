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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    #[test]
    fn test_retry_immediate_success() {
        let result: Result<i32, &str> = retry!(3, 1, Ok(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_retry_succeeds_after_failure() {
        let counter = Cell::new(0);
        let result: Result<i32, &str> = retry!(3, 1, {
            counter.set(counter.get() + 1);
            if counter.get() < 2 {
                Err("not yet")
            } else {
                Ok(42)
            }
        });
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.get(), 2);
    }

    #[test]
    fn test_retry_exhausts_attempts() {
        let counter = Cell::new(0);
        let result: Result<i32, &str> = retry!(3, 1, {
            counter.set(counter.get() + 1);
            Err("always fail")
        });
        assert!(result.is_err());
        assert_eq!(counter.get(), 3);
    }
}
