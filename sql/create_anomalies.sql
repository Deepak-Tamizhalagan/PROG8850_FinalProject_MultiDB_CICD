
USE climate_db;

CREATE TABLE IF NOT EXISTS anomalies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_id INT,               -- id from weather_data if available
    country VARCHAR(10),
    date VARCHAR(10),
    value FLOAT,
    score FLOAT,                 -- anomaly score (negative for more anomalous in sklearn's IsolationForest)
    method VARCHAR(50),          -- 'isolation_forest' or 'zscore'
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
