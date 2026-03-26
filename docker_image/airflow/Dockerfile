
FROM apache/airflow:2.7.2-python3.10

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Chuyển lại về user airflow mặc định để cài thư viện Python (bắt buộc)
USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# --- ĐÂY LÀ ĐOẠN FIX LỖI DATETIME ---
USER root
RUN sed -i 's/datetime.now(timezone.utc)/datetime.now(timezone.utc).replace(tzinfo=None)/g' /home/airflow/.local/lib/python3.8/site-packages/flask_session/sessions.py || true

# Copy patch script
COPY patch_flask_session.sh /tmp/patch_flask_session.sh

# Run patch script as root
USER root
RUN chmod +x /tmp/patch_flask_session.sh && /tmp/patch_flask_session.sh

USER airflow