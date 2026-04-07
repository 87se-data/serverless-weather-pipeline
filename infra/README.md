# 🏗️ Infrastructure Configuration

本プロジェクトのインフラ構成と、再現のための設定メモです。
コスト最適化（FinOps）とスケーラビリティを重視し、Google Cloud のサーバーレス製品を組み合わせて構築しています。

---

## ☁️ Google Cloud Services
使用している主なリソースと選定理由です。

| サービス名 | 用途 | 選定理由 |
| :--- | :--- | :--- |
| **Cloud Run (Service)** | API / SSE 配信 | リクエストがない時は 0 にスケールし、コストを最小化するため。 |
| **Cloud Run (Jobs)** | 気象データ取得・解析 | 1日6回のバッチ処理に特化し、タイムアウト制約が緩いため。 |
| **Cloud Storage (GCS)** | Zarr / npy データ蓄積 | 低コストかつ高性能なオブジェクトストレージ。APIとの親和性が高いため。 |
| **Artifact Registry** | Dockerイメージ管理 | Cloud Run へのデプロイパイプラインの標準化のため。 |

---

