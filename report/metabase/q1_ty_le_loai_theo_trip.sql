select trip_id, drive_id, lighting, conditions, split, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 1) as pct_dropped,
       round(avg_speed_kmh::numeric, 1) as avg_speed_kmh
from marts.mart_trip_summary
order by pct_dropped desc
limit 10
