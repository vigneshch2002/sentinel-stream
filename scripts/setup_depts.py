import urllib.request
import os

# The "Maven" dependencies for 2026
JAR_DEPS = {
    "flink-sql-connector-kafka": "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar",
    "iceberg-flink-runtime": "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.0/iceberg-flink-runtime-1.18-1.5.0.jar",
    "flink-s3-fs-presto": "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-presto/1.18.0/flink-s3-fs-presto-1.18.0.jar"
}

def sync_jars():
    lib_dir = "lib"
    os.makedirs(lib_dir, exist_ok=True)
        
    for name, url in JAR_DEPS.items():
        # Ensure the filename ends in .jar
        filename = f"{name}.jar" if not name.endswith(".jar") else name
        path = os.path.join(lib_dir, filename)
        
        if not os.path.exists(path):
            print(f"📥 Downloading {filename}...")
            urllib.request.urlretrieve(url, path)
        else:
            print(f"✅ {filename} already present.")

if __name__ == "__main__":
    sync_jars()