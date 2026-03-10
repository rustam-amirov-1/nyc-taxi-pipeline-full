FROM apache/airflow:2.8.1

USER root

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt