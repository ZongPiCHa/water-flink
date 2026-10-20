USE water_analytics;

-- Filled by water-flink from the Hudi dimension tables (water.projects /
-- water.community_users via MySQL CDC). NULL means the dimension row was not
-- visible to the lookup join yet.
ALTER TABLE project_engagement_stats
    ADD COLUMN project_name VARCHAR(64) NULL,
    ADD COLUMN author_nickname VARCHAR(32) NULL,
    ADD COLUMN project_published_at DATETIME(3) NULL,
    ALGORITHM = INSTANT;

ALTER TABLE user_engagement_stats
    ADD COLUMN user_nickname VARCHAR(32) NULL,
    ALGORITHM = INSTANT;
