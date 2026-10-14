CREATE DATABASE IF NOT EXISTS water_analytics
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;

USE water_analytics;

CREATE TABLE IF NOT EXISTS heat_score_rule (
    version BIGINT UNSIGNED NOT NULL,
    like_weight INT UNSIGNED NOT NULL,
    comment_weight INT UNSIGNED NOT NULL,
    share_weight INT UNSIGNED NOT NULL,
    follow_weight INT UNSIGNED NOT NULL,
    status TINYINT UNSIGNED NOT NULL COMMENT '0=PENDING,1=ACTIVE,2=DISABLED',
    note VARCHAR(255) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    activated_at DATETIME(3) NULL,
    published_at DATETIME(3) NULL,
    active_slot TINYINT GENERATED ALWAYS AS (
        CASE WHEN status = 1 THEN 1 ELSE NULL END
    ) STORED,
    PRIMARY KEY (version),
    UNIQUE KEY uk_heat_score_rule_active (active_slot),
    CONSTRAINT chk_heat_score_rule_status CHECK (status IN (0, 1, 2))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS user_engagement_stats (
    window_type TINYINT UNSIGNED NOT NULL COMMENT '1=HOUR,2=DAY,3=THREE_DAY,4=WEEK',
    window_start DATETIME(3) NOT NULL,
    window_end DATETIME(3) NOT NULL,
    user_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    like_count BIGINT NOT NULL DEFAULT 0,
    comment_count BIGINT NOT NULL DEFAULT 0,
    share_count BIGINT NOT NULL DEFAULT 0,
    follow_count BIGINT NOT NULL DEFAULT 0,
    heat_score BIGINT NOT NULL DEFAULT 0,
    score_version BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (window_type, window_start, user_id),
    KEY idx_user_history (user_id, window_type, window_start DESC),
    KEY idx_user_window_heat (
        window_type,
        window_start,
        score_version,
        heat_score DESC,
        user_id
    ),
    CONSTRAINT chk_user_window_type CHECK (window_type IN (1, 2, 3, 4))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS project_engagement_stats (
    window_type TINYINT UNSIGNED NOT NULL COMMENT '1=HOUR,2=DAY,3=THREE_DAY,4=WEEK',
    window_start DATETIME(3) NOT NULL,
    window_end DATETIME(3) NOT NULL,
    project_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    user_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    like_count BIGINT NOT NULL DEFAULT 0,
    comment_count BIGINT NOT NULL DEFAULT 0,
    share_count BIGINT NOT NULL DEFAULT 0,
    follow_count BIGINT NOT NULL DEFAULT 0,
    heat_score BIGINT NOT NULL DEFAULT 0,
    score_version BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (window_type, window_start, project_id),
    KEY idx_project_history (project_id, window_type, window_start DESC),
    KEY idx_project_author_window (
        user_id,
        window_type,
        window_start DESC,
        project_id
    ),
    KEY idx_project_window_heat (
        window_type,
        window_start,
        score_version,
        heat_score DESC,
        project_id
    ),
    CONSTRAINT chk_project_window_type CHECK (window_type IN (1, 2, 3, 4))
) ENGINE=InnoDB;

INSERT INTO heat_score_rule (
    version,
    like_weight,
    comment_weight,
    share_weight,
    follow_weight,
    status,
    note,
    activated_at
)
SELECT 1, 1000, 3000, 5000, 4000, 1, 'Initial rule', CURRENT_TIMESTAMP(3)
WHERE NOT EXISTS (SELECT 1 FROM heat_score_rule);

DROP EVENT IF EXISTS cleanup_engagement_stats;

DELIMITER //

CREATE EVENT cleanup_engagement_stats
    ON SCHEDULE EVERY 1 HOUR
    DO
BEGIN
    DELETE FROM user_engagement_stats
    WHERE window_type = 1
      AND window_start < DATE_SUB(NOW(), INTERVAL 24 HOUR);
    DELETE FROM user_engagement_stats
    WHERE window_type = 2
      AND window_start < DATE_SUB(NOW(), INTERVAL 7 DAY);
    DELETE FROM user_engagement_stats
    WHERE window_type = 3
      AND window_start < DATE_SUB(NOW(), INTERVAL 18 DAY);
    DELETE FROM user_engagement_stats
    WHERE window_type = 4
      AND window_start < DATE_SUB(NOW(), INTERVAL 56 DAY);

    DELETE FROM project_engagement_stats
    WHERE window_type = 1
      AND window_start < DATE_SUB(NOW(), INTERVAL 24 HOUR);
    DELETE FROM project_engagement_stats
    WHERE window_type = 2
      AND window_start < DATE_SUB(NOW(), INTERVAL 7 DAY);
    DELETE FROM project_engagement_stats
    WHERE window_type = 3
      AND window_start < DATE_SUB(NOW(), INTERVAL 18 DAY);
    DELETE FROM project_engagement_stats
    WHERE window_type = 4
      AND window_start < DATE_SUB(NOW(), INTERVAL 56 DAY);
END//

DELIMITER ;
