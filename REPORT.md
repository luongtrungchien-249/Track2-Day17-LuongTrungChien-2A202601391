# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Lương Trung Chiến  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 35.6s
run 2/3 … 33.4s
run 3/3 … 36.0s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
──────────────────────────────────────────────────────────────────────────
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks       ✓ ok              31,200      31,200   ✓
quarantine_tickets    ✓ ok                 312         312   ✓

CHECKSUM từng lượt
──────────────────────────────────────────────────────────────────────────
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

KIỂM TRA KHÁC
──────────────────────────────────────────────────────────────────────────
dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
  số file parquet                           ✗ 5,000 → 5,000
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓  1 · gold_training_set idempotent & đúng số hàng
✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓  3 · contract + quarantine + dbt test
✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**. Output trên được lưu trước khi thực hiện bài mở rộng A; kết quả tối ưu dashboard sau đó được ghi tại mục 4.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau lần chạy đầu bảng có 13.790 hàng nhưng chỉ 12.480 `ticket_id`; chạy lần hai tăng lên 26.270 hàng trong khi số ticket vẫn là 12.480. Baseline ba lượt đạt 38.750 hàng và checksum thay đổi. |
| **Nguyên nhân** | `gold_training_set` là incremental model nhưng không khai báo `unique_key` và strategy, nên dbt append thay vì nhận diện cùng một entity. Nguồn CDC có 1.310 record `op='u'`, vì vậy một ticket có thể được ghi ở ngày tạo và ngày cập nhật; retry tiếp tục nhân bản dữ liệu. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, đặt `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong `dags/ai_training_pipeline.py`, đặt `catchup=False`, `max_active_runs=1` để tránh backfill và các DAG run ghi đồng thời. |
| **Bằng chứng** | trước: 13.790 → 26.270 hàng sau hai lượt · sau: 12.480 hàng/12.480 ticket, không lặp · checksum 3 lượt: `8dd7c98653` giống nhau. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ổn định nhưng chỉ có 8.645/9.100 hàng, thiếu đúng 455 cặp `(event_date, customer_id)` trong các ngày 03–13/08. Khoảng 5,05% event đến muộn hơn một ngày. |
| **P99 độ trễ đo được** | **2,7258 ngày** (xấp xỉ 65,42 giờ); độ trễ lớn nhất đo được là 2,9447 ngày. |
| **Lookback đã chọn** | 3 ngày — làm tròn P99 lên ngày nguyên; trong bộ dữ liệu này window cũng bao phủ giá trị max. |
| **Nguyên nhân** | Điều kiện `event_date > max(event_date)` giả định event đến đúng thứ tự thời gian. Event ngày 12/08 đến kho ngày 15/08 không lớn hơn ngày lớn nhất đã có nên bị bỏ vĩnh viễn. Khi mở rộng window, nếu vẫn append thì các khóa được tính lại sẽ bị trùng. |
| **Cách khắc phục** | Trong `gold_feature_daily.sql`, dùng lookback 3 ngày, khai báo khóa ghép `['event_date', 'customer_id']` và strategy `merge` để lần tính lại thay thế kết quả cũ. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng, 0 khóa ghép trùng; chạy thêm một pipeline vẫn 9.100 · checksum 3 lượt: `3db448685c`. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 cân bằng độ đầy đủ và chi phí tính lại, ít bị một outlier cực đoan kéo window quá rộng. Dùng `max` giảm nguy cơ bỏ sót nhưng một outlier có thể buộc mọi lượt chạy sau quét lại lịch sử rất dài. Dùng P99 có rủi ro bỏ sót phần đuôi khoảng 1%, nên cần theo dõi late-arrival và có cơ chế backfill; trong dữ liệu đo được, lookback 3 ngày vẫn bao phủ cả max 2,9447 ngày.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 10/08, nguồn chuyển từ số sang nhãn chữ. Silver có 6.606 hàng sai: 6.488 NULL cùng các giá trị `-1`, `0`, `5`; quarantine rỗng và 9 test cũ vẫn pass. |
| **Nguyên nhân** | `try_cast` biến `urgent/high/medium/low` hợp lệ thành NULL nhưng lại chấp nhận số ngoài miền `-1/0/5`. Contract đang tắt và thiếu test miền giá trị, nên pipeline không lỗi kỹ thuật nhưng dữ liệu downstream sai ngữ nghĩa. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | (1) `1/2/3/4`: giữ nguyên; (2) `urgent/high/medium/low`: map thành `1/2/3/4`; (3) `P1/P2/unknown/0/5/-1/chuỗi rỗng/NULL`: trả NULL và đưa vào quarantine. Chỉ nhóm 3 có đúng 312 record lỗi thật. |
| **Cách khắc phục** | Viết `normalize_priority` dùng chung; trong `silver_tickets` lọc record không chuẩn hóa được **trước** khi rank để giữ trạng thái hợp lệ trước đó; `quarantine_tickets` nhận các record macro trả NULL; bật contract và thêm `not_null`, `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` 11/11 pass · `silver_tickets` = 12.480 ticket và 0 priority sai · `gold_training_set` vẫn 12.480 hàng. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Bronze nên giữ nguyên payload để audit, replay và điều tra nguồn; chuẩn hóa và quyết định hợp lệ thuộc Silver. Không nên để 312 record lỗi chặn hơn 130.000 event và 31.200 document chunk hợp lệ. Quarantine cô lập lỗi để vận hành xử lý, trong khi pipeline vẫn cung cấp dữ liệu tốt cho downstream.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A và B |
| **Nguyên nhân** | **A:** 130.683 hàng bị phân tán trong 5.000 file nhỏ không partition; query bọc `event_time` bằng `strftime`, nên DuckDB mở toàn bộ file và quét 5.000.000 đơn vị. **B:** consumer commit offset trước khi ghi; crash giữa hai thao tác làm mất batch (at-most-once). |
| **Cách khắc phục** | **A:** compact thành 14 partition `event_date`, sort theo `event_date, customer_name, event_time`, row group 2.048; bật hive partitioning và filter sargable `event_date = DATE '2026-08-09'`. **B:** thêm primary key `event_id`, ghi trước/commit sau và dùng `ON CONFLICT DO UPDATE` để replay idempotent, đồng thời cập nhật được message có nội dung mới. |
| **Bằng chứng** | **A:** rows scanned 5.000.000 → 9.324 (giảm 536,3×), file 5.000 → 14, rows on disk giữ 130.683, hash giữ `4379e4c5d9f3`. **B:** crash batch 7 tại offset 3.000; restart kết thúc 20.000 hàng/20.000 ID, không mất, không trùng, strict test **ĐẠT**. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain, natural key và semantics ghi/retry của từng incremental model trước khi nhìn vào scheduler. |
| 2 | So sánh event time với ingestion time, đo phân bố độ trễ và kiểm tra watermark/lookback có bỏ dữ liệu đến muộn không. |
| 3 | So sánh phân bố giá trị Bronze–Silver, kiểm tra contract, test miền giá trị và đường đi của record bị từ chối. |
