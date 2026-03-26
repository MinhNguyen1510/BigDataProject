FROM apache/superset:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y \
        libsasl2-dev \
        libsasl2-modules \
        libkrb5-dev \
        gcc \
        g++ \
        python3-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Fix lỗi packaging và cài đặt các thư viện kết nối chuẩn
RUN pip3 install --no-cache-dir --upgrade pip wheel && \
    pip3 install --no-cache-dir "packaging<24" && \
    pip3 install --no-cache-dir \
        "thrift>=0.11.0,<1.0" \
        "sasl>=0.3.1" \
        "thrift-sasl>=0.4.1" \
        "PyHive[hive]>=0.7.0"

USER superset