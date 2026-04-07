# 🌤️ Wx Pro: Serverless Weather Data Pipeline

気象庁の生データ（GRIB2形式）を独自の高速エンジンで解析し、Androidアプリへリアルタイムにストリーミング配信（SSE）する、完全サーバーレスなデータパイプラインです。極限までのコスト最適化と処理の高速化を両立しています。

---

## 🎯 Architecture Overview

[cite_start]本プロジェクト **Wx Pro** は、RDBや常時稼働サーバーを完全に排除した**「ゼロスケール構成」**を採用しています [cite: 5-100]。

1. **データ取得・解析 (Cython / Python)**
   * [cite_start]気象庁 MSM モデル等のバイナリデータ（GRIB2）を独自ロジックで高速パース [cite: 5]。
2. **クラウドストレージ保存 (GCS)**
   * [cite_start]解析済みデータを **Zarr / npy** 形式に変換し、Google Cloud Storage (GCS) へ保存 [cite: 5]。
3. **API配信 (Cloud Run / FastAPI)**
   * [cite_start]GCS からデータをインメモリ展開し、FastAPI を用いて **Server-Sent Events (SSE)** で配信 [cite: 5]。
4. **クライアント (Android / Kotlin)**
   * [cite_start]**Jetpack Compose** を採用したモダンな UI により、リアルタイムに情報を反映 [cite: 5]。

---

## 🚀 Technical Highlights

### ⚡ 高速 GRIB2 デコーダー (Cython 実装)
[cite_start]本プロジェクトの核心は、Cython で独自に実装された高パフォーマンスな GRIB2 デコーダーです [cite: 5]。気象庁（JMA）の膨大なデータセットをネイティブコードに近い速度で処理するために設計されています。

> [!IMPORTANT]
> **パフォーマンス最適化に関する注記:**
> [cite_start]* **型付きメモリビュー (Typed Memoryviews)**: C 言語レベルのメモリ直接参照を活用し、Python オブジェクトアクセスのオーバーヘッドを完全に排除 [cite: 71, 72]。
> [cite_start]* **高度な圧縮への対応**: **コンポジット圧縮（テンプレート 5.3）** 等に対応し、再帰的な **2 階差分 (Second-order difference)** による復元ロジックを実装 [cite: 50, 68, 88]。
> * **知的財産保護**: これらの核心的な最適化ロジックは独自の技術資産であるため、本パブリックリポジトリではスタブコードに置き換えています。

### 🏗️ サーバーレス・アーキテクチャの徹底
* [cite_start]**完全ステートレス設計**: 固定費のかかるインフラを廃止し、GCS と Cloud Run を組み合わせることで**インフラ維持費を実質ゼロ**に削減 [cite: 5]。
* **マルチステージビルド**: Dockerfile においてビルド用と実行用のステージを分離し、コンテナイメージの軽量化と Cloud Run の起動高速化を実現。

---

## 📈 Key Achievements（実績）

* **劇的な処理速度の向上**
  * [cite_start]Cython の導入とビットストリーム・パースの最適化による高速デコード [cite: 5]。
* **FinOps（クラウドコスト最適化）**
  * [cite_start]リクエストがない時はリソースを 0 にスケールさせる構成により、個人開発における運用コストを極限まで抑制 [cite: 5]。
* **AI-Driven Development**
  * [cite_start]アーキテクチャ設計は自律的に行いつつ、実装やセキュリティ監査においては LLM（Gemini / Cursor Roo）をフル活用し、開発スピードを圧倒的に加速 [cite: 5]。

---

## 📺 Demo
> [!TIP]
> ※ ここに後日、Android 実機の動作動画（GIF/MP4）を貼り付ける予定です。

---

### ⚠️ データの出典および利用に関する免責事項
* [cite_start]本システムにおいて利用している気象データ（MSM 等）は、京都大学生存圏研究所の生存圏データベースを通じて取得した気象庁データを利用しています [cite: 5]。
* [cite_start]本リポジトリは、開発者個人の技術習得ならびに技術検証を目的とした「非商用」の成果物です。商用サービスとして一般提供するものではありません [cite: 5]。
* [cite_start]本リポジトリにて気象生データ（GRIB2 等）の再配布は一切行っておりません [cite: 5]。

---
*Created by [87se-data](https://github.com/87se-data)*