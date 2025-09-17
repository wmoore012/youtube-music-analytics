-- Configuration Management Database Schema
-- This schema supports the configuration management system for scoring parameters

-- Table for storing scoring algorithm metadata
CREATE TABLE IF NOT EXISTS scoring_algorithms (
    algorithm_id VARCHAR(50) PRIMARY KEY,
    algorithm_name VARCHAR(100) NOT NULL UNIQUE,
    version VARCHAR(20) NOT NULL,
    description TEXT,
    status ENUM('active', 'inactive', 'deprecated', 'testing') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_algorithm_name (algorithm_name),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

-- Table for storing scoring configurations per environment
CREATE TABLE IF NOT EXISTS scoring_configurations (
    config_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    algorithm_id VARCHAR(50) NOT NULL,
    environment VARCHAR(50) NOT NULL,
    parameters JSON NOT NULL,
    status ENUM('active', 'inactive', 'draft', 'archived') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (algorithm_id) REFERENCES scoring_algorithms(algorithm_id) ON DELETE CASCADE,
    UNIQUE KEY unique_env_algorithm (algorithm_id, environment),
    INDEX idx_environment (environment),
    INDEX idx_config_status (status),
    INDEX idx_updated_at (updated_at)
);

-- Table for auditing configuration changes
CREATE TABLE IF NOT EXISTS configuration_audit_log (
    audit_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    algorithm_name VARCHAR(100) NOT NULL,
    parameter_name VARCHAR(100) NOT NULL,
    old_value JSON,
    new_value JSON,
    changed_by VARCHAR(100) NOT NULL,
    change_reason TEXT,
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    environment VARCHAR(50) NOT NULL,

    INDEX idx_algorithm_timestamp (algorithm_name, change_timestamp),
    INDEX idx_changed_by (changed_by),
    INDEX idx_environment_audit (environment),
    INDEX idx_parameter_name (parameter_name)
);

-- Table for storing environment-specific settings
CREATE TABLE IF NOT EXISTS environment_settings (
    setting_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    environment VARCHAR(50) NOT NULL,
    setting_name VARCHAR(100) NOT NULL,
    setting_value TEXT NOT NULL,
    setting_type ENUM('string', 'integer', 'float', 'enabled_disabled', 'json') DEFAULT 'string',
    description TEXT,
    status ENUM('active', 'inactive', 'deprecated') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_env_setting (environment, setting_name),
    INDEX idx_environment_settings (environment),
    INDEX idx_setting_name (setting_name),
    INDEX idx_setting_status (status)
);

-- Insert default scoring algorithms
INSERT IGNORE INTO scoring_algorithms (algorithm_id, algorithm_name, version, description) VALUES
('momentum_v1', 'momentum_scoring', '1.0.0', 'Artist momentum scoring based on view trends and engagement'),
('engagement_v1', 'engagement_scoring', '1.0.0', 'Engagement scoring based on comments, likes, and sentiment'),
('growth_v1', 'growth_potential', '1.0.0', 'Growth potential scoring based on historical performance trends');

-- Insert default configurations for development environment
INSERT IGNORE INTO scoring_configurations (algorithm_id, environment, parameters) VALUES
('momentum_v1', 'development', JSON_OBJECT(
    'threshold', 0.5,
    'window_days', 30,
    'min_videos', 5
)),
('engagement_v1', 'development', JSON_OBJECT(
    'min_comments', 10,
    'sentiment_weight', 0.7,
    'like_ratio_weight', 0.3
)),
('growth_v1', 'development', JSON_OBJECT(
    'lookback_months', 6,
    'growth_threshold', 0.15,
    'min_data_points', 10
));

-- Insert default configurations for production environment
INSERT IGNORE INTO scoring_configurations (algorithm_id, environment, parameters) VALUES
('momentum_v1', 'production', JSON_OBJECT(
    'threshold', 0.6,
    'window_days', 45,
    'min_videos', 10
)),
('engagement_v1', 'production', JSON_OBJECT(
    'min_comments', 5,
    'sentiment_weight', 0.8,
    'like_ratio_weight', 0.2
)),
('growth_v1', 'production', JSON_OBJECT(
    'lookback_months', 12,
    'growth_threshold', 0.20,
    'min_data_points', 15
));

-- Insert default environment settings
INSERT IGNORE INTO environment_settings (environment, setting_name, setting_value, setting_type, description) VALUES
('development', 'debug_mode', 'enabled', 'enabled_disabled', 'Enable debug logging and verbose output'),
('development', 'max_workers', '2', 'integer', 'Maximum number of worker processes'),
('development', 'scoring_timeout_seconds', '120', 'integer', 'Timeout for scoring operations'),
('development', 'cache_mode', 'enabled', 'enabled_disabled', 'Enable configuration caching'),
('development', 'audit_mode', 'enabled', 'enabled_disabled', 'Enable configuration change auditing'),

('staging', 'debug_mode', 'disabled', 'enabled_disabled', 'Enable debug logging and verbose output'),
('staging', 'max_workers', '4', 'integer', 'Maximum number of worker processes'),
('staging', 'scoring_timeout_seconds', '300', 'integer', 'Timeout for scoring operations'),
('staging', 'cache_mode', 'enabled', 'enabled_disabled', 'Enable configuration caching'),
('staging', 'audit_mode', 'enabled', 'enabled_disabled', 'Enable configuration change auditing'),

('production', 'debug_mode', 'disabled', 'enabled_disabled', 'Enable debug logging and verbose output'),
('production', 'max_workers', '8', 'integer', 'Maximum number of worker processes'),
('production', 'scoring_timeout_seconds', '600', 'integer', 'Timeout for scoring operations'),
('production', 'cache_mode', 'enabled', 'enabled_disabled', 'Enable configuration caching'),
('production', 'audit_mode', 'enabled', 'enabled_disabled', 'Enable configuration change auditing');

-- Create views for easier querying

-- View for active algorithm configurations
CREATE OR REPLACE VIEW active_algorithm_configs AS
SELECT
    sa.algorithm_name,
    sa.version,
    sa.description,
    sc.environment,
    sc.parameters,
    sc.updated_at as config_updated_at
FROM scoring_algorithms sa
JOIN scoring_configurations sc ON sa.algorithm_id = sc.algorithm_id
WHERE sa.status = 'active' AND sc.status = 'active';

-- View for recent configuration changes
CREATE OR REPLACE VIEW recent_config_changes AS
SELECT
    algorithm_name,
    parameter_name,
    old_value,
    new_value,
    changed_by,
    change_reason,
    change_timestamp,
    environment
FROM configuration_audit_log
WHERE change_timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
ORDER BY change_timestamp DESC;

-- View for environment settings summary
CREATE OR REPLACE VIEW environment_settings_summary AS
SELECT
    environment,
    COUNT(*) as total_settings,
    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_settings,
    MAX(updated_at) as last_updated
FROM environment_settings
GROUP BY environment;
