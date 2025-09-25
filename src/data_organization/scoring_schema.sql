-- Scoring system database schema
-- This schema supports the data organization and scoring system

-- Table for storing scoring algorithm metadata
CREATE TABLE scoring_algorithms (
    algorithm_id VARCHAR(50) PRIMARY KEY,
    algorithm_name VARCHAR(100) NOT NULL UNIQUE,
    version VARCHAR(20) NOT NULL,
    description TEXT,
    author VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_algorithm_name (algorithm_name),
    INDEX idx_active (is_active),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Table for storing scoring algorithm configurations per environment
CREATE TABLE scoring_configurations (
    config_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    algorithm_id VARCHAR(50) NOT NULL,
    environment VARCHAR(50) NOT NULL DEFAULT 'default',
    parameters JSON NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (algorithm_id) REFERENCES scoring_algorithms(algorithm_id) ON DELETE CASCADE,
    UNIQUE KEY unique_env_algorithm (algorithm_id, environment),
    INDEX idx_environment (environment),
    INDEX idx_active_config (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Table for storing scoring run metadata
CREATE TABLE scoring_runs (
    run_id VARCHAR(50) PRIMARY KEY,
    algorithm_id VARCHAR(50) NOT NULL,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    input_record_count INT NOT NULL DEFAULT 0,
    output_record_count INT NOT NULL DEFAULT 0,
    execution_time_ms INT,
    status ENUM('running', 'completed', 'failed', 'cancelled') NOT NULL DEFAULT 'running',
    error_message TEXT,
    parameters_used JSON,
    metadata JSON,

    FOREIGN KEY (algorithm_id) REFERENCES scoring_algorithms(algorithm_id),
    INDEX idx_timestamp (run_timestamp),
    INDEX idx_status (status),
    INDEX idx_algorithm_run (algorithm_id, run_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Main table for storing scoring results
CREATE TABLE scoring_results (
    result_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    algorithm_id VARCHAR(50) NOT NULL,
    entity_type ENUM('artist', 'video', 'channel', 'comment', 'playlist') NOT NULL,
    entity_id VARCHAR(255) NOT NULL,
    score_type VARCHAR(50) NOT NULL,
    score_value DECIMAL(10,4) NOT NULL,
    confidence_level DECIMAL(5,4),
    calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON,

    FOREIGN KEY (run_id) REFERENCES scoring_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (algorithm_id) REFERENCES scoring_algorithms(algorithm_id),
    INDEX idx_entity_score (entity_type, entity_id, score_type),
    INDEX idx_algorithm_timestamp (algorithm_id, calculation_timestamp),
    INDEX idx_run_results (run_id),
    INDEX idx_score_value (score_value),
    INDEX idx_entity_type (entity_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Table for storing detailed scoring metrics (optional additional data)
CREATE TABLE scoring_metrics (
    metric_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    result_id BIGINT NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,6),
    metric_text VARCHAR(500),

    FOREIGN KEY (result_id) REFERENCES scoring_results(result_id) ON DELETE CASCADE,
    INDEX idx_result_metric (result_id, metric_name),
    INDEX idx_metric_name (metric_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- View for easy querying of latest scoring results per entity
CREATE VIEW latest_scoring_results AS
SELECT
    sr.entity_type,
    sr.entity_id,
    sr.algorithm_id,
    sa.algorithm_name,
    sr.score_type,
    sr.score_value,
    sr.confidence_level,
    sr.calculation_timestamp,
    sr.metadata,
    ROW_NUMBER() OVER (
        PARTITION BY sr.entity_type, sr.entity_id, sr.algorithm_id, sr.score_type
        ORDER BY sr.calculation_timestamp DESC
    ) as rn
FROM scoring_results sr
JOIN scoring_algorithms sa ON sr.algorithm_id = sa.algorithm_id
WHERE sa.is_active = TRUE;

-- View for scoring result summaries with run information
CREATE VIEW scoring_result_summary AS
SELECT
    sr.run_id,
    sr.algorithm_id,
    sa.algorithm_name,
    sa.version,
    srun.run_timestamp,
    srun.status as run_status,
    sr.entity_type,
    COUNT(*) as result_count,
    AVG(sr.score_value) as avg_score,
    MIN(sr.score_value) as min_score,
    MAX(sr.score_value) as max_score,
    AVG(sr.confidence_level) as avg_confidence
FROM scoring_results sr
JOIN scoring_algorithms sa ON sr.algorithm_id = sa.algorithm_id
JOIN scoring_runs srun ON sr.run_id = srun.run_id
GROUP BY sr.run_id, sr.algorithm_id, sa.algorithm_name, sa.version,
         srun.run_timestamp, srun.status, sr.entity_type;
