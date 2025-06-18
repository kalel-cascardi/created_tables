 # List of Actioned Sellers in January 2025

 CREATE TABLE `etsy-data-warehouse-dev.kcascardi.actioned_sellers_jan2025` AS 
 with a as (SELECT
      DISTINCT user_id,
      TIMESTAMP_SECONDS(start_date) as start_date,
      TIMESTAMP_SECONDS(end_date) as end_date,
      action_taken
    FROM
      `etsy-data-warehouse-prod.rollups.ts_suspension_actions`
    WHERE 1=1
    --we only want user ids
      AND target_type='user_id'
    --we want sellers actioned that weren't reinstated
      AND end_date IS NULL
    --only sellers
      and is_seller = 1
    --in Jan 2025
      AND TIMESTAMP_SECONDS(start_date) >= TIMESTAMP("2025-01-01")
      AND TIMESTAMP_SECONDS(start_date) < TIMESTAMP("2025-02-01"))

--I want just the first time they were actioned and not reinstated
select user_id,
  min(start_date) as action_start_date
from a
group by 1;
