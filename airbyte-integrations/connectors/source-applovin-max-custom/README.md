# source-applovin-max

Custom Airbyte source: AppLovin MAX User-Level Ad Revenue API -> BigQuery.
Stream: `User_Level_Ad_Impression_Report` (incremental, cursor `date`).

## Nguon goc
- Code extract tu image `dataezg/source-applovin-max:v1.6.0` (truoc day chua co trong repo).
- `requirements.txt` = pip freeze cua image v1.6.0 (Python 3.9, airbyte-cdk 0.51.1) — build tai tao doc lap, khong phu thuoc image cu.

## v1.7.0 — Changelog
- Fix cursor drift: bo `.add(days=1)` trong state setter (moi sync rong day cursor +1 ngay vao tuong lai -> crash dinh ky).
- Clamp cursor <= today: state hong (ngay tuong lai) tu duoc sua khi sync, khong can reset state.
- `stream_slices` tra `[]` thay vi `[None]` khi khong co slice; guard `stream_slice or {}` trong `request_params` -> het TypeError 'NoneType' object is not iterable.
- Warning khi `list_app` rong.

## Build (tren VM)
docker build -t dataezg/source-applovin-max:v1.7.0 .
docker run --rm dataezg/source-applovin-max:v1.7.0 spec | head -5
docker push dataezg/source-applovin-max:v1.7.0
