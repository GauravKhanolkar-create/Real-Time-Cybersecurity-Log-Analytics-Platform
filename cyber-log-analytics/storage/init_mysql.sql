-- Schema for storing enriched security log events and generated alerts.
-- MySQL equivalent of storage/init_postgres.sql
--
-- Notes on the port from PostgreSQL:
--   - SERIAL            -> INT AUTO_INCREMENT
--   - INET               -> VARCHAR(45)  (MySQL has no native IP type; 45 chars fits IPv6)
--   - TEXT[]  (array)    -> JSON          (MySQL has no native array type)
--   - TIMESTAMPTZ         -> TIMESTAMP    (MySQL TIMESTAMP is UTC-normalized internally)

CREATE TABLE IF NOT EXISTS security_logs (
    log_id CHAR(36) PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    src_ip VARCHAR(45),
    dst_ip VARCHAR(45),
    src_port INT,
    dst_port INT,
    protocol VARCHAR(10),
    event_type VARCHAR(30),
    action VARCHAR(30),
    app_user VARCHAR(100),
    bytes_sent BIGINT,
    bytes_received BIGINT,
    duration_ms INT,
    failed_login_count_5m INT,
    unique_ports_contacted_1m INT,
    triggered_rules JSON,
    rule_severity VARCHAR(20),
    ml_anomaly_score DOUBLE,
    ml_flagged BOOLEAN,
    final_severity VARCHAR(20),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_security_logs_event_time (event_time),
    INDEX idx_security_logs_src_ip (src_ip),
    INDEX idx_security_logs_severity (final_severity)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS security_alerts (
    alert_id INT AUTO_INCREMENT PRIMARY KEY,
    log_id CHAR(36),
    event_time TIMESTAMP NOT NULL,
    src_ip VARCHAR(45),
    app_user VARCHAR(100),
    triggered_rules JSON,
    severity VARCHAR(20),
    ml_anomaly_score DOUBLE,
    notified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_security_alerts_severity (severity),
    INDEX idx_security_alerts_notified (notified),
    CONSTRAINT fk_security_alerts_log_id
        FOREIGN KEY (log_id) REFERENCES security_logs (log_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
