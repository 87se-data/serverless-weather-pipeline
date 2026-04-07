# DebianベースのPython 3.10 slimイメージを使用
FROM python:3.10-slim

# 作業ディレクトリを設定
WORKDIR /app

# 依存関係ファイル（requirements.txt）を先にコピー
COPY ./requirements.txt ./

# apt-getを更新し、必要なパッケージをインストール
# libpq-dev: psycopg2（PostgreSQL接続）のビルドまたは実行に必要
# locales: 言語設定に必要なパッケージ
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev \
    locales && \
    rm -rf /var/lib/apt/lists/*

# ja_JP.UTF-8ロケールを有効にする
RUN localedef -i ja_JP -f UTF-8 ja_JP.UTF-8

# pipとwheelを最新化
RUN python3 -m pip install --upgrade pip wheel

# requirements.txtの依存関係をインストール
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# アプリケーションのソースコードをコピー
COPY ./main.py ./

# コンテナがリッスンするポートを指定
EXPOSE 8080

# アプリケーションの起動コマンド
CMD ["uvicorn", "main:app", "--host=0.0.0.0", "--port=8080"]