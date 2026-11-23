-- Databricks notebook source
-- DBTITLE 1,Overview
-- MAGIC %md
-- MAGIC # Salesforce Gold (SQL)
-- MAGIC
-- MAGIC Business layer of the **Salesforce Bronze Ingestion (SQL)** pipeline. It reads the deduplicated silver tables in `silver` and publishes materialized views to `gold`. It has the same tables, columns, comments, keys and expectations as the original PySpark notebook `salesforce_gold`, which is kept in this folder for reference but is not part of the pipeline.
-- MAGIC
-- MAGIC **Run it from the pipeline, not cell by cell.** `CREATE OR REFRESH MATERIALIZED VIEW ... CONSTRAINT ... EXPECT` is pipeline syntax, so running a cell on a SQL warehouse or cluster fails.
-- MAGIC
-- MAGIC | Gold table | Grain | Purpose |
-- MAGIC | --- | --- | --- |
-- MAGIC | `gold.account_360` | one row per `account_id` | Account attributes with revenue, contact, sales pipeline and support KPIs |
-- MAGIC | `gold.opportunities_enriched` | one row per `opportunity_id` | Opportunities with account and primary-contact details, weighted amount, sales cycle |
-- MAGIC | `gold.tickets_enriched` | one row per `ticket_id` | Support tickets with account and reporter details, open flag, priority rank, NPS category |
-- MAGIC | `gold.revenue_by_account_day` | `account_id` x `currency` x `transaction_date` | Daily transaction amounts by status and product type per account |
-- MAGIC | `gold.revenue_by_account_month` | `account_id` x `currency` x `month` | Monthly roll-up of `revenue_by_account_day`, plus `active_days` |
-- MAGIC
-- MAGIC **Business rules**
-- MAGIC * **Mixed currencies:** amounts are never converted. Every amount sits next to its currency (`account_currency` in `account_360`). It becomes `MIXED` if an account ever uses more than one currency.
-- MAGIC * **Win rate** is count-based: won / (won + lost). It is NULL when an account has no closed opportunities.
-- MAGIC * **SLA breach rate** = breached / responded tickets. Tickets without a first response are excluded.
-- MAGIC * **Open tickets** have status New, In Progress or Waiting on Customer.
-- MAGIC * **PII:** `opportunities_enriched` and `tickets_enriched` contain contact names and emails.
-- MAGIC
-- MAGIC Each table has an informational primary key, column comments, and expectations. A missing key fails the update; the range and consistency checks only warn.
-- MAGIC
-- MAGIC **Performance**
-- MAGIC * Every table uses `CLUSTER BY AUTO`: Databricks picks and maintains the liquid clustering keys from the query patterns.
-- MAGIC * All five views are written so they can be refreshed **incrementally**. `account_360` computes its averages as exact `sum / count` instead of `avg()`, because an average over floating point blocks incremental refresh. On each update the pipeline chooses incremental or full recompute by cost; the choice is visible in the event log (`planning_information`).

-- COMMAND ----------

-- DBTITLE 1,gold.account_360
-- account_360: one row per account with revenue, contact, sales pipeline and support KPIs
CREATE OR REFRESH MATERIALIZED VIEW gold.account_360 (
  account_id STRING NOT NULL COMMENT 'Unique account key (ACC#####). Primary key.',
  account_name STRING COMMENT 'Company name.',
  industry STRING COMMENT 'Industry of the account.',
  account_type STRING COMMENT 'Relationship type: Customer, Partner or Prospect.',
  country STRING COMMENT 'Headquarters country; determines the account currency.',
  employee_count INT COMMENT 'Company headcount (size of the customer organisation, not number of contacts).',
  annual_revenue DOUBLE COMMENT 'Estimated annual company revenue, in the account local currency.',
  website STRING COMMENT 'Company website URL.',
  account_created_date DATE COMMENT 'Date the account was created in Salesforce.',
  account_currency STRING COMMENT 'Currency of all amounts in this row (from transactions and opportunities). MIXED if the account uses more than one currency; NULL if it has none.',
  contact_count BIGINT COMMENT 'Number of contacts at the account.',
  transaction_count BIGINT COMMENT 'Number of transactions (all statuses).',
  completed_transaction_count BIGINT COMMENT 'Transactions with status Completed.',
  pending_transaction_count BIGINT COMMENT 'Transactions with status Pending.',
  refunded_transaction_count BIGINT COMMENT 'Transactions with status Refunded.',
  completed_amount DECIMAL(28,2) COMMENT 'Sum of Completed transaction amounts (in account_currency, not converted). Realised revenue.',
  pending_amount DECIMAL(28,2) COMMENT 'Sum of Pending transaction amounts (in account_currency, not converted).',
  refunded_amount DECIMAL(28,2) COMMENT 'Sum of Refunded transaction amounts (in account_currency, not converted).',
  first_transaction_date DATE COMMENT 'Date of the first transaction.',
  last_transaction_date DATE COMMENT 'Date of the most recent transaction.',
  opportunity_count BIGINT COMMENT 'Number of opportunities (all stages).',
  open_opportunity_count BIGINT COMMENT 'Opportunities not yet closed.',
  won_opportunity_count BIGINT COMMENT 'Opportunities in stage Closed Won.',
  lost_opportunity_count BIGINT COMMENT 'Opportunities in stage Closed Lost.',
  open_pipeline_amount DECIMAL(28,2) COMMENT 'Sum of open opportunity amounts (in account_currency, not converted).',
  weighted_open_pipeline_amount DECIMAL(28,2) COMMENT 'Sum of open opportunity amount x probability / 100 (in account_currency, not converted).',
  won_amount DECIMAL(28,2) COMMENT 'Sum of Closed Won opportunity amounts (in account_currency, not converted).',
  win_rate DOUBLE COMMENT 'Count-based win rate: won / (won + lost), 0-1. NULL when no opportunity is closed.',
  ticket_count BIGINT COMMENT 'Number of support tickets.',
  open_ticket_count BIGINT COMMENT 'Tickets with status New, In Progress or Waiting on Customer.',
  p1_ticket_count BIGINT COMMENT 'Tickets with priority P1 - Critical.',
  bug_ticket_count BIGINT COMMENT 'Tickets that are confirmed product bugs.',
  escalated_ticket_count BIGINT COMMENT 'Tickets that were escalated.',
  responded_ticket_count BIGINT COMMENT 'Tickets with a first response (denominator of sla_breach_rate).',
  sla_breached_ticket_count BIGINT COMMENT 'Responded tickets whose first response exceeded the SLA target.',
  sla_breach_rate DOUBLE COMMENT 'sla_breached_ticket_count / responded_ticket_count, 0-1. NULL when no ticket was responded to.',
  csat_response_count BIGINT COMMENT 'Tickets with a CSAT survey answer.',
  avg_csat DOUBLE COMMENT 'Average CSAT score (1-5) over tickets with a survey answer.',
  avg_nps DOUBLE COMMENT 'Average NPS answer (0-10) over tickets with a survey answer.',
  avg_resolution_hours DOUBLE COMMENT 'Average hours from creation to resolution over resolved tickets.',
  last_ticket_created_at TIMESTAMP COMMENT 'Creation time of the most recent ticket (can be in the future: some tickets are intentionally future-dated).',
  CONSTRAINT account_id_present EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT single_currency EXPECT (account_currency IS NULL OR account_currency <> 'MIXED'),
  CONSTRAINT win_rate_in_range EXPECT (win_rate IS NULL OR win_rate BETWEEN 0 AND 1),
  CONSTRAINT sla_breach_rate_in_range EXPECT (sla_breach_rate IS NULL OR sla_breach_rate BETWEEN 0 AND 1),
  CONSTRAINT avg_csat_in_range EXPECT (avg_csat IS NULL OR avg_csat BETWEEN 1 AND 5),
  CONSTRAINT avg_nps_in_range EXPECT (avg_nps IS NULL OR avg_nps BETWEEN 0 AND 10),
  CONSTRAINT transaction_counts_consistent EXPECT (completed_transaction_count + pending_transaction_count + refunded_transaction_count = transaction_count),
  CONSTRAINT opportunity_counts_consistent EXPECT (open_opportunity_count + won_opportunity_count + lost_opportunity_count = opportunity_count),
  CONSTRAINT open_tickets_le_total EXPECT (open_ticket_count <= ticket_count),
  CONSTRAINT weighted_le_open_pipeline EXPECT (weighted_open_pipeline_amount <= open_pipeline_amount),
  CONSTRAINT gold_account_360_pk PRIMARY KEY (account_id)
)
CLUSTER BY AUTO
COMMENT 'Gold account 360: one row per Salesforce account with attributes plus revenue, contact, sales pipeline and support KPIs. Amounts are in the original currency (see the currency column) and are never converted: do not sum them across currencies. All amounts in a row are in account_currency.'
AS
WITH contacts AS (
  SELECT account_id, count(*) AS contact_count
  FROM silver.sf_contacts
  GROUP BY account_id
),
txn AS (
  SELECT
    account_id,
    count(*) AS transaction_count,
    sum(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed_transaction_count,
    sum(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) AS pending_transaction_count,
    sum(CASE WHEN status = 'Refunded' THEN 1 ELSE 0 END) AS refunded_transaction_count,
    sum(CASE WHEN status = 'Completed' THEN amount END) AS completed_amount,
    sum(CASE WHEN status = 'Pending' THEN amount END) AS pending_amount,
    sum(CASE WHEN status = 'Refunded' THEN amount END) AS refunded_amount,
    min(transaction_date) AS first_transaction_date,
    max(transaction_date) AS last_transaction_date,
    collect_set(currency) AS txn_currencies
  FROM silver.sf_transactions
  GROUP BY account_id
),
opp_flags AS (
  SELECT
    *,
    NOT coalesce(is_closed, false) AS flag_open,
    coalesce(is_won, false) AS flag_won,
    coalesce(is_closed, false) AND NOT coalesce(is_won, false) AS flag_lost
  FROM silver.sf_opportunities
),
opp AS (
  SELECT
    account_id,
    count(*) AS opportunity_count,
    sum(CASE WHEN flag_open THEN 1 ELSE 0 END) AS open_opportunity_count,
    sum(CASE WHEN flag_won THEN 1 ELSE 0 END) AS won_opportunity_count,
    sum(CASE WHEN flag_lost THEN 1 ELSE 0 END) AS lost_opportunity_count,
    sum(CASE WHEN flag_open THEN amount END) AS open_pipeline_amount,
    sum(CASE WHEN flag_open THEN amount * probability / 100 END) AS weighted_open_pipeline_amount,
    sum(CASE WHEN flag_won THEN amount END) AS won_amount,
    collect_set(currency) AS opp_currencies
  FROM opp_flags
  GROUP BY account_id
),
tix AS (
  SELECT
    account_id,
    count(*) AS ticket_count,
    sum(CASE WHEN status IN ('New', 'In Progress', 'Waiting on Customer') THEN 1 ELSE 0 END) AS open_ticket_count,
    sum(CASE WHEN startswith(priority, 'P1') THEN 1 ELSE 0 END) AS p1_ticket_count,
    sum(CASE WHEN coalesce(is_bug, false) THEN 1 ELSE 0 END) AS bug_ticket_count,
    sum(CASE WHEN coalesce(escalated, false) THEN 1 ELSE 0 END) AS escalated_ticket_count,
    sum(CASE WHEN first_response_minutes IS NOT NULL THEN 1 ELSE 0 END) AS responded_ticket_count,
    sum(CASE WHEN first_response_minutes IS NOT NULL AND coalesce(sla_breached, false) THEN 1 ELSE 0 END) AS sla_breached_ticket_count,
    -- exact sums and counts instead of avg(): an average over floating point blocks incremental refresh
    count(csat_score) AS csat_response_count,
    sum(csat_score) AS csat_score_sum,
    count(nps_score) AS nps_response_count,
    sum(nps_score) AS nps_score_sum,
    count(resolution_hours) AS resolved_hours_count,
    sum(CAST(resolution_hours AS DECIMAL(18,2))) AS resolution_hours_sum,
    max(created_at) AS last_ticket_created_at
  FROM silver.sf_tickets
  GROUP BY account_id
),
joined AS (
  SELECT
    a.account_id, a.account_name, a.industry, a.account_type, a.country, a.employee_count, a.annual_revenue, a.website,
    a.created_date AS account_created_date,
    array_distinct(concat(coalesce(t.txn_currencies, CAST(array() AS ARRAY<STRING>)),
                          coalesce(o.opp_currencies, CAST(array() AS ARRAY<STRING>)))) AS currencies,
    coalesce(c.contact_count, 0) AS contact_count,
    coalesce(t.transaction_count, 0) AS transaction_count,
    coalesce(t.completed_transaction_count, 0) AS completed_transaction_count,
    coalesce(t.pending_transaction_count, 0) AS pending_transaction_count,
    coalesce(t.refunded_transaction_count, 0) AS refunded_transaction_count,
    coalesce(t.completed_amount, 0) AS completed_amount,
    coalesce(t.pending_amount, 0) AS pending_amount,
    coalesce(t.refunded_amount, 0) AS refunded_amount,
    t.first_transaction_date, t.last_transaction_date,
    coalesce(o.opportunity_count, 0) AS opportunity_count,
    coalesce(o.open_opportunity_count, 0) AS open_opportunity_count,
    coalesce(o.won_opportunity_count, 0) AS won_opportunity_count,
    coalesce(o.lost_opportunity_count, 0) AS lost_opportunity_count,
    coalesce(o.open_pipeline_amount, 0) AS open_pipeline_amount,
    coalesce(o.weighted_open_pipeline_amount, 0) AS weighted_open_pipeline_amount,
    coalesce(o.won_amount, 0) AS won_amount,
    coalesce(k.ticket_count, 0) AS ticket_count,
    coalesce(k.open_ticket_count, 0) AS open_ticket_count,
    coalesce(k.p1_ticket_count, 0) AS p1_ticket_count,
    coalesce(k.bug_ticket_count, 0) AS bug_ticket_count,
    coalesce(k.escalated_ticket_count, 0) AS escalated_ticket_count,
    coalesce(k.responded_ticket_count, 0) AS responded_ticket_count,
    coalesce(k.sla_breached_ticket_count, 0) AS sla_breached_ticket_count,
    coalesce(k.csat_response_count, 0) AS csat_response_count,
    k.csat_score_sum, k.nps_response_count, k.nps_score_sum, k.resolved_hours_count, k.resolution_hours_sum,
    k.last_ticket_created_at
  FROM silver.sf_accounts a
  LEFT JOIN contacts c ON c.account_id = a.account_id
  LEFT JOIN txn t ON t.account_id = a.account_id
  LEFT JOIN opp o ON o.account_id = a.account_id
  LEFT JOIN tix k ON k.account_id = a.account_id
)
SELECT
  account_id, account_name, industry, account_type, country, employee_count, annual_revenue, website, account_created_date,
  CASE WHEN size(currencies) = 1 THEN element_at(currencies, 1) WHEN size(currencies) > 1 THEN 'MIXED' END AS account_currency,
  CAST(contact_count AS BIGINT) AS contact_count,
  CAST(transaction_count AS BIGINT) AS transaction_count,
  CAST(completed_transaction_count AS BIGINT) AS completed_transaction_count,
  CAST(pending_transaction_count AS BIGINT) AS pending_transaction_count,
  CAST(refunded_transaction_count AS BIGINT) AS refunded_transaction_count,
  CAST(completed_amount AS DECIMAL(28,2)) AS completed_amount,
  CAST(pending_amount AS DECIMAL(28,2)) AS pending_amount,
  CAST(refunded_amount AS DECIMAL(28,2)) AS refunded_amount,
  first_transaction_date, last_transaction_date,
  CAST(opportunity_count AS BIGINT) AS opportunity_count,
  CAST(open_opportunity_count AS BIGINT) AS open_opportunity_count,
  CAST(won_opportunity_count AS BIGINT) AS won_opportunity_count,
  CAST(lost_opportunity_count AS BIGINT) AS lost_opportunity_count,
  CAST(open_pipeline_amount AS DECIMAL(28,2)) AS open_pipeline_amount,
  CAST(weighted_open_pipeline_amount AS DECIMAL(28,2)) AS weighted_open_pipeline_amount,
  CAST(won_amount AS DECIMAL(28,2)) AS won_amount,
  CASE WHEN won_opportunity_count + lost_opportunity_count > 0
       THEN won_opportunity_count / (won_opportunity_count + lost_opportunity_count) END AS win_rate,
  CAST(ticket_count AS BIGINT) AS ticket_count,
  CAST(open_ticket_count AS BIGINT) AS open_ticket_count,
  CAST(p1_ticket_count AS BIGINT) AS p1_ticket_count,
  CAST(bug_ticket_count AS BIGINT) AS bug_ticket_count,
  CAST(escalated_ticket_count AS BIGINT) AS escalated_ticket_count,
  CAST(responded_ticket_count AS BIGINT) AS responded_ticket_count,
  CAST(sla_breached_ticket_count AS BIGINT) AS sla_breached_ticket_count,
  CASE WHEN responded_ticket_count > 0 THEN sla_breached_ticket_count / responded_ticket_count END AS sla_breach_rate,
  CAST(csat_response_count AS BIGINT) AS csat_response_count,
  -- averages = exact sum / count, computed per row after aggregation (keeps the MV incrementally refreshable)
  CASE WHEN csat_response_count > 0 THEN csat_score_sum / csat_response_count END AS avg_csat,
  CASE WHEN nps_response_count > 0 THEN nps_score_sum / nps_response_count END AS avg_nps,
  CASE WHEN resolved_hours_count > 0 THEN CAST(resolution_hours_sum AS DOUBLE) / resolved_hours_count END AS avg_resolution_hours,
  last_ticket_created_at
FROM joined;

-- COMMAND ----------

-- DBTITLE 1,gold.opportunities_enriched
-- opportunities_enriched: one row per opportunity with account and primary-contact details
CREATE OR REFRESH MATERIALIZED VIEW gold.opportunities_enriched (
  opportunity_id STRING NOT NULL COMMENT 'Unique opportunity key (OPP######). Primary key.',
  account_id STRING COMMENT 'Account the opportunity belongs to (FK to account_360.account_id).',
  account_name STRING COMMENT 'Company name of the account.',
  industry STRING COMMENT 'Industry of the account.',
  account_type STRING COMMENT 'Relationship type of the account: Customer, Partner or Prospect.',
  country STRING COMMENT 'Headquarters country of the account.',
  opportunity_name STRING COMMENT 'Opportunity name: account - Upwind product (type).',
  type STRING COMMENT 'Deal type: New Business, Expansion or Renewal.',
  stage STRING COMMENT 'Sales stage: Prospecting, Qualification, Needs Analysis, Proposal, Negotiation, Closed Won, Closed Lost.',
  is_open BOOLEAN COMMENT 'True while the opportunity is not closed.',
  is_closed BOOLEAN COMMENT 'True when stage is Closed Won or Closed Lost.',
  is_won BOOLEAN COMMENT 'True when stage is Closed Won.',
  probability INT COMMENT 'Win probability in percent (0-100), set by stage.',
  amount DECIMAL(18,2) COMMENT 'Deal amount in the currency column (not converted).',
  weighted_amount DECIMAL(20,2) COMMENT 'amount x probability / 100, in the currency column (not converted).',
  currency STRING COMMENT 'ISO currency code of amount and weighted_amount.',
  lead_source STRING COMMENT 'Where the deal came from: Web, Partner Referral, Event, Outbound, Inbound.',
  primary_contact_id STRING COMMENT 'Main customer contact (FK to silver.sf_contacts.contact_id).',
  primary_contact_name STRING COMMENT 'Full name of the primary contact. PII.',
  primary_contact_title STRING COMMENT 'Job title of the primary contact.',
  primary_contact_email STRING COMMENT 'Work email of the primary contact. PII.',
  created_date DATE COMMENT 'Date the opportunity was created.',
  close_date DATE COMMENT 'Actual close date for closed deals, expected close date for open deals.',
  close_month DATE COMMENT 'First day of the close_date month, for monthly pipeline and bookings reporting.',
  sales_cycle_days INT COMMENT 'Days from created_date to close_date (actual for closed, expected for open deals).',
  CONSTRAINT opportunity_id_present EXPECT (opportunity_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT account_found EXPECT (account_name IS NOT NULL),
  CONSTRAINT primary_contact_found EXPECT (primary_contact_id IS NULL OR primary_contact_name IS NOT NULL),
  CONSTRAINT probability_in_range EXPECT (probability BETWEEN 0 AND 100),
  CONSTRAINT weighted_le_amount EXPECT (weighted_amount <= amount),
  CONSTRAINT close_after_created EXPECT (sales_cycle_days IS NULL OR sales_cycle_days >= 0),
  CONSTRAINT gold_opportunities_enriched_pk PRIMARY KEY (opportunity_id)
)
CLUSTER BY AUTO
COMMENT 'Gold opportunities: one row per Salesforce opportunity with account and primary-contact details, open flag, weighted amount and sales cycle. Amounts are in the original currency (see the currency column) and are never converted: do not sum them across currencies. Contains PII (contact name and email).'
AS
WITH contacts AS (
  SELECT
    contact_id AS primary_contact_id,
    concat_ws(' ', first_name, last_name) AS primary_contact_name,
    title AS primary_contact_title,
    email AS primary_contact_email
  FROM silver.sf_contacts
)
SELECT
  o.opportunity_id, o.account_id, a.account_name, a.industry, a.account_type, a.country,
  o.opportunity_name, o.type, o.stage,
  NOT coalesce(o.is_closed, false) AS is_open,
  o.is_closed, o.is_won, o.probability, o.amount,
  CAST(o.amount * o.probability / 100 AS DECIMAL(20,2)) AS weighted_amount,
  o.currency, o.lead_source, o.primary_contact_id,
  c.primary_contact_name, c.primary_contact_title, c.primary_contact_email,
  o.created_date, o.close_date,
  trunc(o.close_date, 'MM') AS close_month,
  CAST(datediff(o.close_date, o.created_date) AS INT) AS sales_cycle_days
FROM silver.sf_opportunities o
LEFT JOIN silver.sf_accounts a ON a.account_id = o.account_id
LEFT JOIN contacts c ON c.primary_contact_id = o.primary_contact_id;

-- COMMAND ----------

-- DBTITLE 1,gold.tickets_enriched
-- tickets_enriched: one row per support ticket with account and reporter details
CREATE OR REFRESH MATERIALIZED VIEW gold.tickets_enriched (
  ticket_id STRING NOT NULL COMMENT 'Unique support ticket key (TKT######). Primary key.',
  account_id STRING COMMENT 'Account that opened the ticket (FK to account_360.account_id).',
  account_name STRING COMMENT 'Company name of the account.',
  industry STRING COMMENT 'Industry of the account.',
  account_type STRING COMMENT 'Relationship type of the account: Customer, Partner or Prospect.',
  country STRING COMMENT 'Headquarters country of the account.',
  reporter_contact_id STRING COMMENT 'Contact who reported the ticket (FK to silver.sf_contacts.contact_id).',
  reporter_name STRING COMMENT 'Full name of the reporting contact. PII.',
  reporter_title STRING COMMENT 'Job title of the reporting contact.',
  reporter_email STRING COMMENT 'Work email of the reporting contact. PII.',
  created_at TIMESTAMP COMMENT 'Timestamp the ticket was opened (some tickets are intentionally future-dated).',
  created_month DATE COMMENT 'First day of the month the ticket was opened, for trend reporting.',
  first_response_at TIMESTAMP COMMENT 'Timestamp of the first support response; NULL if not answered yet.',
  resolved_at TIMESTAMP COMMENT 'Timestamp the ticket was resolved; NULL while open.',
  status STRING COMMENT 'Lifecycle status: New, In Progress, Waiting on Customer, Resolved, Closed.',
  is_open BOOLEAN COMMENT 'True when status is New, In Progress or Waiting on Customer.',
  priority STRING COMMENT 'Business priority: P1 - Critical, P2 - High, P3 - Medium, P4 - Low.',
  priority_rank INT COMMENT 'Numeric priority 1-4 parsed from priority (1 = Critical), for sorting.',
  ticket_type STRING COMMENT 'Bug, Incident, Configuration, Question or Feature Request.',
  product_module STRING COMMENT 'Upwind module affected (Runtime Sensor, CSPM, Vulnerability Management, KSPM, Cloud Detection & Response, Compliance & Reporting, API Security, Integrations, Console & Access).',
  subject STRING COMMENT 'Short ticket title describing the problem.',
  cloud_provider STRING COMMENT 'Customer cloud involved: AWS, Azure, GCP, Multi-cloud, or N/A.',
  environment STRING COMMENT 'Customer environment affected: Production, Staging or Development.',
  channel STRING COMMENT 'How the ticket was opened: Portal, Email, Slack Connect, In-app, Phone.',
  product_version STRING COMMENT 'Upwind platform or sensor version reported by the customer.',
  affected_assets INT COMMENT 'Number of customer cloud assets impacted.',
  root_cause STRING COMMENT 'Root cause after investigation; Under Investigation while open.',
  is_bug BOOLEAN COMMENT 'True when the ticket is a confirmed product bug.',
  bug_id STRING COMMENT 'Engineering bug reference (UPW-####) when is_bug is true.',
  bug_severity STRING COMMENT 'Bug severity S1-S4 when is_bug is true.',
  support_tier STRING COMMENT 'Support tier handling the ticket: Tier 1, Tier 2 or Tier 3.',
  escalated BOOLEAN COMMENT 'True when the ticket was escalated.',
  reopen_count INT COMMENT 'Number of times the ticket was reopened after being resolved.',
  sla_first_response_minutes INT COMMENT 'First-response SLA target in minutes for the priority.',
  first_response_minutes INT COMMENT 'Actual minutes from creation to first response; NULL if not answered yet.',
  sla_breached BOOLEAN COMMENT 'True when first_response_minutes exceeded sla_first_response_minutes.',
  resolution_hours DOUBLE COMMENT 'Hours from creation to resolution; NULL while open.',
  csat_score INT COMMENT 'Customer satisfaction 1-5; only on resolved tickets with a survey response.',
  nps_score INT COMMENT 'Net Promoter answer 0-10.',
  nps_category STRING COMMENT 'Promoter (9-10), Passive (7-8) or Detractor (0-6); NULL without an NPS answer.',
  sentiment STRING COMMENT 'Feedback sentiment derived from csat_score: Positive, Neutral, Negative.',
  feedback_comment STRING COMMENT 'Free-text customer feedback from the satisfaction survey.',
  CONSTRAINT ticket_id_present EXPECT (ticket_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT account_found EXPECT (account_name IS NOT NULL),
  CONSTRAINT reporter_found EXPECT (reporter_contact_id IS NULL OR reporter_name IS NOT NULL),
  CONSTRAINT priority_rank_in_range EXPECT (priority_rank BETWEEN 1 AND 4),
  CONSTRAINT csat_in_range EXPECT (csat_score IS NULL OR csat_score BETWEEN 1 AND 5),
  CONSTRAINT nps_in_range EXPECT (nps_score IS NULL OR nps_score BETWEEN 0 AND 10),
  CONSTRAINT open_has_no_resolution EXPECT (NOT is_open OR resolved_at IS NULL),
  CONSTRAINT gold_tickets_enriched_pk PRIMARY KEY (ticket_id)
)
CLUSTER BY AUTO
COMMENT 'Gold support tickets: one row per Upwind support ticket with account and reporter details, open flag, priority rank and NPS category. Used for stability and customer satisfaction analysis. Contains PII (reporter name and email).'
AS
WITH contacts AS (
  SELECT
    contact_id AS reporter_contact_id,
    concat_ws(' ', first_name, last_name) AS reporter_name,
    title AS reporter_title,
    email AS reporter_email
  FROM silver.sf_contacts
)
SELECT
  t.ticket_id, t.account_id, a.account_name, a.industry, a.account_type, a.country,
  t.reporter_contact_id, c.reporter_name, c.reporter_title, c.reporter_email,
  t.created_at,
  trunc(to_date(t.created_at), 'MM') AS created_month,
  t.first_response_at, t.resolved_at, t.status,
  t.status IN ('New', 'In Progress', 'Waiting on Customer') AS is_open,
  t.priority,
  CAST(substring(t.priority, 2, 1) AS INT) AS priority_rank,
  t.ticket_type, t.product_module, t.subject, t.cloud_provider, t.environment, t.channel, t.product_version,
  t.affected_assets, t.root_cause, t.is_bug, t.bug_id, t.bug_severity, t.support_tier, t.escalated, t.reopen_count,
  t.sla_first_response_minutes, t.first_response_minutes, t.sla_breached, t.resolution_hours,
  t.csat_score, t.nps_score,
  CASE WHEN t.nps_score >= 9 THEN 'Promoter'
       WHEN t.nps_score >= 7 THEN 'Passive'
       WHEN t.nps_score IS NOT NULL THEN 'Detractor' END AS nps_category,
  t.sentiment, t.feedback_comment
FROM silver.sf_tickets t
LEFT JOIN silver.sf_accounts a ON a.account_id = t.account_id
LEFT JOIN contacts c ON c.reporter_contact_id = t.reporter_contact_id;