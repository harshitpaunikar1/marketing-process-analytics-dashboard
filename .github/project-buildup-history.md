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
## 2024-08-19 - Day 3: Data pipeline design

- Task summary: Created a schema diagram for the unified marketing events table that the dashboard will read from. Included field descriptions and data type notes.
- Deliverable: Unified events table schema created and documented.
## 2024-08-26 - Day 4: Dashboard mockup

- Task summary: Built the dashboard mockup for the Marketing Process Analytics project. Used a simple Figma-style layout documented in the case study with annotations showing what each panel shows and why. The four key panels are: campaign performance overview, channel attribution breakdown, lead pipeline health, and cost per acquisition trend. Added explanatory notes for why each view was chosen over alternatives.
- Deliverable: Dashboard mockup complete with four panels and design rationale documented.
## 2024-08-26 - Day 4: Dashboard mockup

- Task summary: Revised the channel attribution panel after realizing last-touch attribution was too simplistic for this use case. Replaced with a first-touch and multi-touch comparison view.
- Deliverable: Attribution panel revised from last-touch to multi-touch comparison.
