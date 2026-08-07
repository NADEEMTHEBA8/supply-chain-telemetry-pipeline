-- =============================================================================
-- Predictive Supply Chain Telemetry Pipeline — Databricks SQL Dashboard Queries
-- =============================================================================
-- Connect these queries in Databricks SQL → Dashboards to build an executive 
-- real-time 24/7 Supply Chain Risk Monitoring Dashboard.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Query 1: Top 10 High-Risk Machine Leaderboard (Counter & Table Widget)
-- -----------------------------------------------------------------------------
SELECT 
    machine_id, 
    plant_id, 
    ROUND(risk_score, 4) AS risk_score, 
    fault_event_count_24h, 
    overheat_count_24h, 
    vibration_anomaly_count_24h, 
    ROUND(avg_temp_24h, 1) AS avg_temp_24h, 
    ROUND(max_temp_24h, 1) AS max_temp_24h,
    event_date
FROM default.gold_supply_risk
ORDER BY risk_score DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Query 2: Plant Site Operational Risk Summary (Bar Chart Widget)
-- -----------------------------------------------------------------------------
SELECT 
    plant_id, 
    COUNT(DISTINCT machine_id) AS total_machines, 
    ROUND(AVG(risk_score), 4) AS avg_plant_risk_score, 
    SUM(fault_event_count_24h) AS total_faults_24h, 
    SUM(overheat_count_24h) AS total_overheats_24h
FROM default.gold_supply_risk
GROUP BY plant_id
ORDER BY avg_plant_risk_score DESC;

-- -----------------------------------------------------------------------------
-- Query 3: Immediate Maintenance Priority KPI Counter (Stat Widget)
-- -----------------------------------------------------------------------------
SELECT 
    COUNT(CASE WHEN risk_score > 0.30 THEN 1 END) AS high_risk_machine_count,
    COUNT(CASE WHEN risk_score <= 0.30 THEN 1 END) AS healthy_machine_count,
    ROUND(MAX(risk_score), 4) AS peak_risk_score
FROM default.gold_supply_risk;
