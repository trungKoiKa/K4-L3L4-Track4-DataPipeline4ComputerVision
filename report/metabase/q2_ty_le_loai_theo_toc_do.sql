select case when avg_speed_kmh < 20 then '1. < 20 km/h'
            when avg_speed_kmh < 50 then '2. 20-50 km/h'
            else '3. >= 50 km/h' end as speed_band,
       count(*) as trips, sum(frames) as frames, sum(frames_kept) as kept,
       round(100.0 * sum(frames - frames_kept) / sum(frames), 1) as pct_dropped
from marts.mart_trip_summary
group by 1
order by 1
