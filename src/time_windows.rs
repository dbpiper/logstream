use chrono::{Duration as ChronoDuration, NaiveTime, Utc};

pub fn utc_day_range_ms(day_offset: u32) -> (i64, i64) {
    let day = Utc::now().date_naive() - ChronoDuration::days(day_offset as i64);

    let start = day
        .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap_or_default())
        .and_utc()
        .timestamp_millis();

    let end = (day + ChronoDuration::days(1))
        .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap_or_default())
        .and_utc()
        .timestamp_millis();

    (start, end)
}

pub fn utc_prev_day_bounds_ms() -> (i64, i64) {
    let now = Utc::now();
    let today = now.date_naive();
    let start_naive = (today - ChronoDuration::days(1))
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let end_naive = today.and_hms_opt(0, 0, 0).unwrap();
    let start = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(start_naive, Utc);
    let end = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(end_naive, Utc);
    (start.timestamp_millis(), end.timestamp_millis())
}
