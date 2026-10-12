# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

|                                                           |                                                               |
| --------------------------------------------------------- | ------------------------------------------------------------- |
| Họ và tên                                                 | `Hoàng Trung Anh`                                             |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`)            | `2A202602521`                                                 |
| GitHub                                                    | [trungKoiKa (Hoang Trung Anh)](https://github.com/trungKoiKa) |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-06570`                                                  |

> Cách tính trip: MSSV có chữ (`2A…`) nên lệnh `int(MSSV) % 26` của GUIDE không chạy được. Tôi lấy phần số `202602521`: `202602521 % 26 = 17`, và phần tử thứ 17 của `cli list-trips` là `trip-06570`.

## Kết quả cuối

- `dataset_version` của bạn: `725461e2e2e8`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `723 / 151 / 145` (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: `27/27 checks passed` (lần chạy cuối `scripts/lab.sh step all --reset`, rồi `scripts/lab.sh verify`).
- Ảnh chụp trong `report/screenshots/`:
  - `airflow_graph.png`: Graph xanh, `silver_frames [26]`.
  - `minio_lake.png`: bucket `lake/` với `bronze/`, `silver/`, `gold/`.
  - `minio_gold_dataset.png`: thư mục `gold/datasets/725461e2e2e8/`.
  - `metabase_dashboard.png`: dashboard kèm 2 câu hỏi riêng.
  - Bài tập sập: `crash_log_trip-06570.png`, `crash_graph_red.png`, `crash_graph_after_clear.png`, `crash_log_attempt2.png`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt tôi thấy trong `bronze_raw_telemetry_a` (3021 dòng) và `bronze_raw_telemetry_b` (3234 dòng):

| | Nguồn A | Nguồn B |
|---|---|---|
| Tên cột | `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, gas, brake, steer` | `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, throttle_pct, brake_pct, steering` |
| Thời gian | Chuỗi ISO UTC, ví dụ `2025-10-07T08:00:00.014Z` | Epoch ms, ví dụ `1759933801509` |
| Đơn vị tốc độ | m/s (`1.213`) | km/h (`55.11`) |
| Bàn đạp | Tỉ lệ 0..1 (`gas 0.028`) | Phần trăm (`throttle_pct 2.0`) |
| Đồng hồ | Lệch 0 ms so với video | Chạy nhanh hơn video `telemetry_clock_offset_ms = 1500` (trong `bronze_video_files`) |

Mọi cột trong bronze đều là `VARCHAR`. Ví dụ `typeof(speed_mps)` và `typeof(ts_utc)` đều trả `VARCHAR`, và A có 30 ô `speed_mps` rỗng vẫn được giữ nguyên là chuỗi `''`.

Lý do giữ nguyên như vậy:
- **Không mất gì.** Bronze là bản ghi gốc, chỉ thêm (append-only). Nếu ép kiểu ngay thì một ô hỏng sẽ làm lỗi cả lần nạp, hoặc bị đổi thành NULL một cách im lặng, và mất luôn bằng chứng về giá trị gốc.
- **Mỗi nguồn một bảng** vì hai schema khác nhau. Gộp ở bronze buộc phải chọn tên cột và đơn vị ngay lúc nạp. Nếu sau này phát hiện quy đổi sai (ví dụ quên trừ 1,5 s), ta chỉ cần sửa model silver rồi chạy lại từ bronze, không phải nạp lại file.
- **Nạp lại không nhân đôi.** Lần nạp thứ hai báo `already present` vì file được nhận diện bằng sha256. `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, `rows_added 3021`).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**Cách chuẩn hoá:**
- `stg_telemetry_a` chỉ ép kiểu: `ts_utc` thành `timestamptz`, tốc độ giữ nguyên m/s.
- `stg_telemetry_b` đổi tên cột về schema chung, đổi `spd_kmh / 3.6` sang m/s, và đổi `throttle_pct/100`, `brake_pct/100` về tỉ lệ 0..1.
- Thời gian của B được tính bằng `to_timestamp((timestamp_ms - offset_ms)/1000)`. Ở đây `offset_ms = 1500` lấy từ `bronze.video_files`, nên độ lệch đồng hồ được trừ ngay trong SQL.
- `silver_telemetry_classified` gộp hai nguồn bằng `union all by name`, rồi gắn `reject_reason` cho từng dòng.

**Bằng chứng đã đúng:**
- Cột `bat_dau` của B là `2025-10-08 14:30:00.009`, chỉ cách giờ bắt đầu video (`14:30:00`) 9 ms. Nếu chưa trừ độ lệch thì giá trị này sẽ là khoảng `14:30:01.5`.
- Tốc độ tối đa là 71 km/h (A) và 100 km/h (B), đều hợp lý.
- Kiểm tra số dòng: sạch + cách ly = nhận về. A: 2968 + 53 = 3021. B: 3181 + 53 = 3234.

| drive | duplicate | speed_missing | speed_out_of_range | tổng cách ly |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 |
| drive_b | 20 | 30 | 3 | 53 |

Sáu dòng `speed_out_of_range` đều có giá trị `250.0` m/s (900 km/h). Ở A chúng nằm ở `_row_no` 400, 1700 và 2500.

**Vì sao vào `rejects` thay vì xoá:** dòng lỗi là thông tin vận hành, ví dụ cảm biến hỏng hoặc thiết bị gửi trùng. Giữ chúng kèm lý do thì:
- vẫn đối soát được "sạch + cách ly = nhận về" (không mất dòng nào một cách im lặng);
- truy được nguồn lỗi nhờ `_batch_id` và `_row_no`;
- nếu luật làm sạch sai (ví dụ ngưỡng 70 m/s quá chặt), có thể sửa luật và lấy lại các dòng đó mà không cần nạp lại.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

**`max_gap_ms` là `warn`.** Khoảng trống quá 1000 ms là cảm biến mất tín hiệu. Đây là hiện tượng có thật ngoài đường (hầm, mất GPS), dữ liệu xung quanh vẫn đúng, chỉ là thiếu một đoạn. Lần chạy của tôi có **2 kết quả WARN**: mỗi drive một khoảng trống, 3073 ms ở A và 3124 ms ở B. Dừng cả pipeline vì điều này thì không đáng; ta chỉ cần biết để xử lý tiếp (các khung trong khoảng đó bị gắn `sync_lost` và loại ở gold).

**`in_range` (0..70 m/s) là `error`.** Một tốc độ vô lý lọt vào silver nghĩa là luật làm sạch đã hỏng. Dữ liệu sai sẽ chảy thẳng vào dataset huấn luyện và dashboard, nên phải dừng lại.

**Thí nghiệm `break-gate`.** Nới `max_speed_mps` từ 70 lên 1000 thì 6 dòng 250 m/s không còn bị cách ly mà lọt vào `silver_telemetry`. Khi đó `dbt test` cho `PASS=5 WARN=1 ERROR=1`, và test `in_range_silver_telemetry_speed_mps__70__0` **bắt được 6 dòng**. Khôi phục luật gốc thì kết quả trở lại `PASS=6 WARN=1 ERROR=0`.

Điều này cho thấy cổng chất lượng là **lớp bảo vệ thứ hai, độc lập với code làm sạch**. Luật cách ly nằm trong model, còn kỳ vọng nằm trong `schema.yml`. Khi ai đó sửa nhầm model, test vẫn chặn được. Trong DAG, `dbt_test_silver` lỗi thì `dbt_run_gold` trở về sau không chạy, nên dữ liệu xấu không đi tiếp.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Theo `idempotency.json` (trip `trip-00003`, 60 khung):

| Cách ghi | Trước | Sau 2 lần chạy lại | Kết quả |
|---|---|---|---|
| Pipeline: Delta overwrite theo `trip_id` | 1246 dòng, vân tay `6c7480dfde7e` | 1246 dòng, vân tay `6c7480dfde7e` | Không đổi |
| Sai: `append` hai lần | 60 khung | 120 dòng | Nhân đôi (2x) |

Lịch sử Delta cho thấy mỗi lần chạy lại là **một commit** `WRITE` với `predicate: trip_id = 'trip-00003'` (version 26 và 27, mỗi lần `rows_added 60`), thay thế đúng phần của trip đó.

**Vì sao an toàn hơn "xoá rồi ghi":**
- Delta overwrite theo predicate là **một giao dịch nguyên tử**: file mới được ghi xong rồi mới commit vào `_delta_log`, và việc bỏ file cũ nằm trong cùng commit đó. Nếu tiến trình chết giữa chừng thì chưa có commit, người đọc vẫn thấy phiên bản cũ đầy đủ, và file dở dang chỉ là rác chưa được tham chiếu.
- Với "DELETE rồi INSERT" thì có hai bước tách rời. Nếu sập sau DELETE, trip đó **mất dữ liệu**. Nếu chạy lại mà quên DELETE thì bị **nhân đôi**.
- Partition theo trip còn cho phép backfill một trip mà không chạm 25 trip còn lại (xem Câu 9), và có time travel để quay về version cũ.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Đen và tối khác nhau thế nào:**
- Khung **đen** có độ sáng trung bình dưới `dark_luma = 12`. Đây là lỗi camera hoặc mất tín hiệu, ảnh không có thông tin, nên **bị loại**. Lần chạy của tôi: A có 6, B có 4.
- Khung **tối** có độ sáng từ 12 đến dưới 40 (`dark_soft`). Đó là đường hầm, bóng râm, hoàng hôn: điều kiện lái thật, ảnh vẫn nhìn được. Pipeline **gắn cờ nhưng giữ lại**: A có 4, B có 61.

Tôi đồng ý. Mô hình tự lái phải học cả cảnh thiếu sáng; bỏ hết khung tối sẽ làm dataset thiên về trời sáng và mô hình yếu đúng ở chỗ nguy hiểm. Giữ cờ `is_dark` còn giúp cân bằng hoặc đánh giá riêng nhóm này. Điểm cần cẩn thận là ngưỡng 12 cố định: một cảnh đêm rất tối có thể bị coi là đen.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

(Đầy đủ: t1 = 109/23, t4 = 269/137, t8 = 438/348, trên tổng 602/644 khung.)

**Tôi chọn ngưỡng 2**, giống pipeline:
- Ngưỡng 0 chỉ bắt khung giống hệt. Ở A nó vẫn bắt được đoạn camera đứng hình 6 s, nhưng bỏ sót các khung gần như giống nhau khi xe đứng hoặc bò chậm.
- Ngưỡng 6 loại 363/602 khung của A (60 %) và 244/644 của B (38 %). Như vậy là đã xoá cả những khung khác nhau thật khi xe đang chạy.

**Phụ thuộc tốc độ:** A chạy chậm (trung bình 38 km/h, 15,3 % mẫu dưới 7 km/h) nên ở mọi ngưỡng đều có nhiều khung trùng hơn B (trung bình 71 km/h). Khi xe chạy nhanh, hai khung cách nhau 0,5 s đã khác nhau nhiều, nên có thể dùng ngưỡng cao hơn. Khi xe chậm hoặc dừng thì nên giữ ngưỡng thấp. Cách tốt hơn là cho ngưỡng phụ thuộc tốc độ, hoặc lấy mẫu theo quãng đường thay vì cố định 2 khung/giây.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Theo `leakage.json`, với 1019 khung dùng được thuộc 26 trip:

| Cách chia | Trip nằm ở nhiều split | Trip có khung ở cả train lẫn test |
|---|---|---|
| Theo khung hình (ngẫu nhiên 70/15/15) | **26/26** | **26** |
| Theo trip (gold của pipeline) | **0** | 0 |

Khi chia theo trip, gold của tôi có 18 trip train, 4 trip val và 4 trip test. Test dbt `no_straddle` cùng kiểm tra verify "no trip is in two splits" đều PASS.

**Vì sao điểm đẹp giả tạo:** khung lấy mẫu cách nhau 0,5 s trong cùng một trip gần như giống hệt nhau (cùng đường, cùng ánh sáng, cùng xe phía trước). Chia theo khung thì với mỗi ảnh test, mô hình đã thấy "anh em sinh đôi" của nó trong train. Điểm test khi đó đo khả năng **ghi nhớ cảnh** chứ không đo khả năng **tổng quát hoá sang cảnh mới**, nên cao hơn thực tế. Khi triển khai lên đường mới, mô hình sẽ tụt điểm. Chia theo trip (theo cảnh) mới mô phỏng đúng tình huống "gặp đoạn đường chưa từng thấy".

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

**Vì sao là băm nội dung:**
- Chạy lại với cùng đầu vào và cùng luật thì ra cùng phiên bản, nên pipeline idempotent đến tận gold.
- Chỉ cần đọc phiên bản là biết hai dataset có giống nhau không, không phụ thuộc giờ chạy hay máy chạy.
- Nội dung đổi thì phiên bản tự đổi, không thể "cùng tên khác ruột".

Ngày giờ hay số đếm thì sẽ tăng mỗi lần chạy dù dữ liệu không đổi, và hai máy có thể đặt cùng số cho hai dataset khác nhau.

**Kết quả các lần chạy của tôi** (luôn là `725461e2e2e8`):
- `step 4`;
- chạy lại `cli export-dataset` (`manifest_sha256` cũng giữ nguyên `1a0c2e6f…`);
- sau bài tập sập và chạy lại;
- sau `step all --reset` lần cuối, chạy lại từ lake trống.

**Vì sao khác bạn bè:** model gold xếp trip bằng `md5(trip_id || LAB_STUDENT_ID)` rồi chia 70/15/15 theo từng drive. MSSV khác thì thứ tự trip khác, nên các trip rơi vào train/val/test khác, và băm nội dung cũng khác. Tôi đã thử đổi MSSV sang `2024000`: được `dataset_version = 8fc131884584` với 703/182/134 khung (của tôi là 723/151/145), dù vẫn 18/4/4 trip và vẫn 1019 khung. Đặt lại `2A202602521` thì quay về đúng `725461e2e2e8`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/725461e2e2e8/manifest.json`, phần `lineage` của lần chạy cuối:

```json
"bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
"silver.frames": 25, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
"input_video_sha256": {
  "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
  "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc" }
```

**Trả lời "dataset này được tạo từ dữ liệu nào?"** Dataset được tạo từ:
- hai video có đúng hai sha256 trên;
- telemetry bronze A và B ở version 0 (nạp một lần);
- bảng `silver.frames` ở version 25 (26 commit, mỗi trip một partition, version 0..25);
- các bảng silver dbt ở version 0.

`rules` ghi thêm luật loại khung `is_black, is_blurry, is_near_dup, not sync_ok` và cách chia `per trip, stratified by drive, md5 order`. `by_drive` là A 436 khung, B 583 khung.

**Để tái tạo sau 3 tháng, cần:**
- sha256 của video đầu vào, để chứng minh đúng file gốc;
- version các bảng Delta, để dùng time travel đọc đúng trạng thái `silver.frames@25` ngay cả khi bảng đã bị ghi đè sau này;
- `rules` cùng các tham số trong `dbt_project.yml` (`dup_hamming 2`, `sync_tol_ms 150`, `dark_luma 12`…);
- MSSV làm salt cho split;
- commit git của code (ví dụ commit nộp bài trong repo này).

Cuối cùng, đối chiếu `dataset_version` và `manifest_sha256` để xác nhận kết quả tái tạo trùng khớp.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Tôi đặt `LAB_FAIL_ONCE=trip-06570`, chạy `day09_reset`, rồi chạy `day09_video_pipeline` (run `manual__2026-10-12T10:39:51`).

**Khi sập:**
- **1 task đỏ**: `silver_frames` map index 17. Log ghi `RuntimeError: simulated crash while extracting trip-06570; run it again` (ảnh `crash_log_trip-06570.png`).
- 25 task `silver_frames` còn lại vẫn **success**.
- **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse` (ảnh `crash_graph_red.png`).

**Cách chạy lại:** tôi chỉ clear **đúng task đỏ** (`silver_frames`, map index 17) kèm downstream ("Clear task" với Downstream). Không clear 25 trip đã xong.

Kết quả là **8 task chạy lại**: `silver_frames[17]` ở attempt 2 cùng 7 task phía sau. Bảng `task_instance` cho thấy 36 task ở try 1 và chỉ 1 task ở try 2. DAG chuyển xanh (ảnh `crash_graph_after_clear.png` và `crash_log_attempt2.png`).

**Dữ liệu không nhân đôi:**
- `silver.frames` có 1246 dòng và 1246 `frame_id` khác nhau.
- `trip-06570` có đúng 45 dòng.
- `cli history silver.frames` chỉ có một commit `WRITE ... predicate trip_id = 'trip-06570'` (`rows_added 45`). Lần sập xảy ra trước khi ghi, nên không để lại dữ liệu dở; lần chạy lại ghi đè đúng partition đó.

**Sau khi chạy lại:** `scripts/lab.sh verify` cho **27/27**, và `dataset_version` vẫn là `725461e2e2e8`, không đổi.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Quy mô ước tính: 2 drive khoảng 10 phút video, tức 1246 khung sau lấy mẫu. 500 xe × khoảng 8 giờ/ngày là khoảng **240 000 lần** khối lượng hiện tại.

1. **Decode video bằng FFmpeg trong một container Airflow vỡ đầu tiên.** Hiện có `LocalExecutor`, `PARALLELISM=2`, `max_active_tis_per_dagrun=1`, nghĩa là xử lý tuần tự từng trip trên CPU; 26 trip đã mất khoảng 1 phút. Decode là nút cổ chai (slide 24). Thay bằng:
   - worker phân tán (Airflow `KubernetesExecutor`/Celery, hoặc Ray/Spark) đặt gần nơi lưu video;
   - decode bằng GPU (NVDEC);
   - lấy mẫu ngay trên xe hoặc ở edge để không phải chuyển toàn bộ video.
2. **DuckDB một file và dbt chạy trên một máy (lớp biến đổi) vỡ tiếp theo.** `lakehouse.duckdb` nằm trong `work/`, mỗi lần `dbt run` lại tính lại toàn bộ bảng silver/gold. Ở quy mô khoảng 10⁸ khung/ngày, cách này vượt quá RAM và thời gian. Thay bằng engine phân tán đọc Delta trên object storage (Spark/Databricks, Trino), model **incremental** theo partition ngày/xe, và tách compute khỏi storage (slide 22–23).
3. **Điều phối kiểu "một DAG, một task mỗi trip" và Postgres/MinIO một node cũng vỡ.** Hàng chục nghìn mapped task mỗi ngày sẽ làm nghẽn scheduler và metadata DB của Airflow. Bảng `mart_frame_gallery` một dòng mỗi khung trong Postgres sẽ phình rất nhanh. MinIO một node thì không đủ dung lượng và băng thông (slide 12, các chữ V: volume, velocity). Thay bằng:
   - ingestion hướng sự kiện (xe upload thì đẩy sự kiện vào Kafka hoặc hàng đợi), DAG chạy theo partition ngày × xe với backfill;
   - S3 hoặc MinIO cluster có lifecycle sang storage lạnh;
   - warehouse cột (BigQuery/Snowflake/ClickHouse) chỉ chứa mart tổng hợp, ảnh để ở object storage và truy cập bằng URL.

## Câu hỏi riêng của bạn trên Metabase

Tôi thêm hai câu SQL vào dashboard `Day 9: video data pipeline` (ảnh `metabase_dashboard.png`, SQL lưu ở `report/metabase/`).

**Q1 — Tỷ lệ khung bị loại theo trip (top 10):**

```sql
select trip_id, drive_id, lighting, conditions, split, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 1) as pct_dropped,
       round(avg_speed_kmh::numeric, 1) as avg_speed_kmh
from marts.mart_trip_summary order by pct_dropped desc limit 10
```

Trip bị loại nhiều nhất:

| Trip | Bị loại | Tốc độ trung bình |
|---|---|---|
| `trip-04622` | 56,7 % (26/60 khung giữ lại) | 67 km/h |
| `trip-03904` | 50 % | |
| `trip-02086` | 46,7 % | 17,6 km/h |
| `trip-08888` | 40 % | 6,5 km/h |

Cả 4 trip này đều thuộc drive A. Tra ngược trong silver thì lý do loại **gần như chỉ là khung trùng**: 04622 có 34 khung trùng, 02086 có 28, 08888 có 24, và không có khung đen hay mờ. Điều này hợp lý với 08888 và 02086 vì xe gần như đứng yên. Nhưng `trip-04622` chạy 67 km/h mà vẫn có 34 khung trùng, khả năng cao là đoạn camera đứng hình 6 s cộng với cảnh xa lộ đơn điệu. Trip này là ứng viên để kiểm tra bằng mắt xem dHash có loại nhầm hay không.

**Q2 — Tỷ lệ loại theo dải tốc độ** (biểu đồ cột trên `mart_trip_summary`):

| Dải tốc độ | Số trip | Bị loại |
|---|---|---|
| < 20 km/h | 5 | 32,4 % |
| 20–50 km/h | 7 | 17,1 % |
| ≥ 50 km/h | 14 | 14,4 % |

Kết quả xác nhận bằng số điều ở Câu 5: xe càng chậm thì càng nhiều khung trùng bị loại. Dataset vì thế bị thiếu tình huống tốc độ thấp (đô thị, kẹt xe), nên cần lấy mẫu theo quãng đường hoặc nới ngưỡng cho nhóm này.

**Thí nghiệm riêng (điểm cộng).** Tôi chạy `dbt run --select silver_frame_telemetry --vars '{sync_tol_ms: N}'`, không sửa file nào; bằng chứng ở `report/evidence/own_sync_tol.json`.

| `sync_tol_ms` | Khung mất đồng bộ, A | Khung mất đồng bộ, B |
|---|---|---|
| 50 | 272 (45,2 %) | 328 (50,9 %) |
| 100 | 137 (22,8 %) | 187 (29,0 %) |
| 150 (mặc định) | 7 (1,2 %) | 7 (1,1 %) |
| 300 | 7 | 7 |

Telemetry khoảng 10 Hz, nên khoảng cách tới mẫu gần nhất phía trước lên tới khoảng 100 ms cộng jitter. Ngưỡng dưới 100 ms sẽ loại nhầm một nửa dataset. Ngưỡng 150 ms nằm ngay trên chu kỳ lấy mẫu; 7 khung còn lại mỗi drive chính là các đoạn mất tín hiệu khoảng 3 s, tăng lên 300 ms cũng không cứu được.

## Nhìn lại

- **Bất ngờ nhất:** chia theo khung hình làm **26/26 trip** bị rò rỉ. Không có trip nào an toàn, nên một điểm test "đẹp" trong trường hợp này hoàn toàn vô nghĩa. Cũng bất ngờ là `sync_tol_ms` chỉ giảm từ 150 xuống 100 mà đã mất khoảng 25 % khung.
- **Sự cố môi trường:** trên Windows, Git đổi file sang CRLF làm container Airflow không khởi động được (`exec /airflow-entrypoint.sh: no such file or directory`) và làm lệch hash các file được bảo vệ. Tôi phải đặt `core.autocrlf=false` rồi checkout lại với LF. Ngoài ra `check_submission.py --save-verify` không gọi được `lab.sh` trên Windows, nên tôi lưu `verify.txt` bằng chính `scripts/lab.sh verify` chạy qua bash.
- **Chưa rõ:** `verify` báo khung đứng hình bắt được 11/12 (vẫn PASS). Tôi chưa rõ khung còn lại bị bỏ sót vì dHash khác hơn 2 bit hay vì nó trùng với một khung đã bị loại vì lý do khác.
- **Lỗi dữ liệu pipeline chưa bắt được:**
  - Luật tốc độ chỉ kiểm tra khoảng 0..70 m/s, không kiểm tra **gia tốc vô lý giữa hai mẫu liền nhau** (ví dụ nhảy 30 km/h trong 0,1 s), cũng không kiểm tra GPS "nhảy" xa bất thường.
  - dHash có thể loại nhầm khung trên xa lộ đơn điệu, như `trip-04622` chạy 67 km/h mà vẫn bị loại 57 %.
  - Độ lệch đồng hồ của B được **khai báo** (1500 ms) chứ không được **đo**. Nếu thiết bị trôi đồng hồ trong chuyến đi, pipeline sẽ không phát hiện ra.
