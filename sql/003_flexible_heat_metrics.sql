USE water_analytics;
SET SESSION lock_wait_timeout = 10;

ALTER TABLE user_engagement_stats
    ADD COLUMN view_page_count BIGINT NOT NULL DEFAULT 0,
    ALGORITHM = INSTANT;
ALTER TABLE project_engagement_stats
    ADD COLUMN view_page_count BIGINT NOT NULL DEFAULT 0,
    ALGORITHM = INSTANT;

-- JSON weights use integer thousandths, exactly like heat_score.
-- Existing rule versions retain their original meaning (view weight = 0).
ALTER TABLE heat_score_rule ADD COLUMN weights JSON NULL, ALGORITHM = INSTANT;
UPDATE heat_score_rule
SET weights = JSON_OBJECT(
    'like', like_weight, 'comment', comment_weight,
    'share', share_weight, 'follow', follow_weight, 'view_page', 0);
ALTER TABLE heat_score_rule
    ADD COLUMN view_page_weight INT GENERATED ALWAYS AS (
        COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(weights, '$.view_page')) AS SIGNED), 0)
    ) VIRTUAL;

-- Publish a NEW version through the panel/API after upgrading Flink:
-- weights={"like":1,"comment":3,"share":5,"follow":4,"view_page":0.1}
-- The API accepts display units; DB/Kafka store weights multiplied by 1000.
