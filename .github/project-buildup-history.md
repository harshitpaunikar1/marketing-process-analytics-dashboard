# Project Buildup History: Marketing Process Analytics Dashboard

- Repository: `marketing-process-analytics-dashboard`
- Category: `product_case_study`
- Subtype: `generic`
- Source: `project_buildup_2021_2025_daily_plan_extra.csv`
## 2024-08-19 - Day 3: Data pipeline design

- Task summary: Worked on the data pipeline design for the Marketing Process Analytics Dashboard. The dashboard needs to pull from three source systems — CRM, marketing automation platform, and web analytics. Sketched the data flow diagram and identified the join keys between each system. The main challenge is that customer IDs are not consistent across all three — CRM uses an internal ID, the marketing platform uses email, and web analytics uses a cookie ID. Designed a probabilistic matching step to reconcile the three.
- Deliverable: Data flow designed. ID reconciliation approach using probabilistic matching documented.
## 2024-08-19 - Day 3: Data pipeline design

- Task summary: Added a data freshness requirement to the pipeline design — the dashboard needs to reflect events within a 4-hour window, which rules out batch processing approaches and requires near-realtime or micro-batch.
- Deliverable: 4-hour freshness requirement documented. Micro-batch approach selected over nightly batch.
