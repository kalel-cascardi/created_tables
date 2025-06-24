# Main Table 
CREATE TABLE `etsy-data-warehouse-dev.kcascardi.graph_ml_02242025` AS 

-- Step 1: Base query to get raw connections and dates with refined logic
WITH YourBaseData AS (
    with graph_data as (
      select seed_id, 
        entity_id, 
        edge_type
      from `etsy-tns-ml-dev.zlong.tg_bfs_hop4_results_query_fix`
      where hops_from_seed in (2,4)
    ),

    -- CTE 'a': Gets the LAST action for seeds suspended IN Jan 2025
    a as (
      SELECT seller_user_id, 
      max_start_date, 
      last_action_taken FROM (
        SELECT user_id as seller_user_id, 
        TIMESTAMP_SECONDS(start_date) as max_start_date, 
        action_taken as last_action_taken,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY TIMESTAMP_SECONDS(start_date) DESC) as rn
        FROM `etsy-data-warehouse-prod.rollups.ts_suspension_actions`
        WHERE 1=1 
        AND target_type='user_id' 
        AND end_date IS NULL 
        AND is_seller = 1
        AND TIMESTAMP_SECONDS(start_date) >= TIMESTAMP("2025-01-01") 
        AND TIMESTAMP_SECONDS(start_date) < TIMESTAMP("2025-02-01")
      ) WHERE rn = 1
    ),

    -- CTE 'b': Gets the FIRST action for entities suspended AFTER Jan 2025
    b as (
      SELECT seller_user_id, 
      min_start_date, 
      first_action_taken FROM (
        SELECT user_id as seller_user_id, 
        TIMESTAMP_SECONDS(start_date) as min_start_date, 
        action_taken as first_action_taken,
               -- We use ASC here to get the FIRST action
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY TIMESTAMP_SECONDS(start_date) ASC) as rn
        FROM `etsy-data-warehouse-prod.rollups.ts_suspension_actions`
        WHERE 1=1
        AND target_type='user_id' 
        AND end_date IS NULL 
        AND is_seller = 1
        AND TIMESTAMP_SECONDS(start_date) >= TIMESTAMP("2025-01-01")
      ) WHERE rn = 1
    )
    select
      g.seed_id,
      a.max_start_date as seed_action_date,
      a.last_action_taken as seed_action,
      g.entity_id,
      b.min_start_date as entity_action_date,
      b.first_action_taken as entity_action,
      g.edge_type
    from graph_data g
    -- INNER JOIN ensures we only have connections where both parties meet our criteria
    left join a on a.seller_user_id = g.seed_id
    left join b on b.seller_user_id = g.entity_id
),

-- Step 2: Standardize user pairs and aggregate edge types
FinalData AS (
    SELECT
      CONCAT(CAST(seed_id AS STRING), '_', CAST(entity_id AS STRING)) AS connection_id,
      MAX(seed_action_date) as user_1_date,
      MAX(seed_action) as user_1_action,
      MAX(entity_action_date) as user_2_date,
      MAX(entity_action) as user_2_action,
      STRING_AGG(DISTINCT edge_type, ', ') AS all_edge_types
    FROM YourBaseData
    GROUP BY connection_id
),

-- Step 3: Calculate the precise "at-risk" CoR
AtRiskCoR AS (
  SELECT
    fd.connection_id,
    SUM(cor.amount_usd) as at_risk_cor_amount
  FROM FinalData fd
  JOIN `etsy-data-warehouse-prod.rollups.cor_main` cor
    -- Join on the entity (user_2) ID
    ON CAST(cor.seller_user_id AS STRING) = SPLIT(fd.connection_id, '_')[SAFE_OFFSET(1)]
  WHERE
    -- Only include refunds AFTER the first user (seed) was actioned
    DATE(TIMESTAMP_SECONDS(cor.order_date)) > DATE(fd.user_1_date)
    AND cor.cor_stream NOT IN ('etsy_covered_refund')
  GROUP BY
    fd.connection_id
),

-- Final Step: Join all data and present the complete table
table_1 as (SELECT
  fd.connection_id,
  SPLIT(fd.connection_id, '_')[SAFE_OFFSET(0)] AS user_1_id,
  fd.user_1_date,
  fd.user_1_action,
  SPLIT(fd.connection_id, '_')[SAFE_OFFSET(1)] AS user_2_id,
  fd.user_2_date,
  fd.user_2_action,
  fd.all_edge_types,
  DATE_DIFF(fd.user_2_date, fd.user_1_date, DAY) AS lead_time_days,
  IFNULL(risk.at_risk_cor_amount, 0) as at_risk_cor
FROM FinalData fd
LEFT JOIN AtRiskCoR risk ON risk.connection_id = fd.connection_id
ORDER BY
  lead_time_days DESC)



  -- This query calculates all your final KPIs in a single row
-- Just replace "YourFinalAnalysisTable" with the name of the table you saved the above results to
-- Or, wrap the entire query from above in a CTE called "YourFinalAnalysisTable"

SELECT 
*
  -- -- Metric 1: Overall Scope
  -- COUNT(*) AS total_predictive_connections,

  -- -- Metric 2: The Time-Based Opportunity
  -- ROUND(AVG(lead_time_days), 1) AS avg_lead_time_days,
  -- APPROX_QUANTILES(lead_time_days, 100)[OFFSET(50)] AS median_lead_time_days,
  -- COUNTIF(lead_time_days >= 30) AS count_lead_time_over_30_days,

  -- -- Metric 3: The Financial Opportunity
  -- ROUND(SUM(at_risk_cor), 2) AS total_at_risk_refunds,
  -- COUNTIF(at_risk_cor > 0) AS connections_with_financial_risk,
  -- ROUND(AVG(IF(at_risk_cor > 0, at_risk_cor, NULL)), 2) AS avg_cor_per_risky_connection

FROM table_1;
