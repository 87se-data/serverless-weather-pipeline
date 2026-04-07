# 使用するPythonのバージョン
FROM python:3.11-slim

# 作業ディレクトリの設定
WORKDIR /app

# 依存関係のインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ソースコードのコピー
COPY main.py .

# Cloud Runでポートを指定できるように環境変数を設定（デフォルト 8080）
ENV PORT 8080

# コンテナ起動時にFastAPI（Uvicorn）を起動
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
