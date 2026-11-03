-- Databricks notebook source
-- DBTITLE 1,Overview
-- MAGIC %md
-- MAGIC # Salesforce Silver
-- MAGIC
-- MAGIC Silver layer of the **Salesforce Bronze Ingestion (SQL)** pipeline. Each table holds **exactly one row per primary key**, built with `AUTO CDC` (SCD type 1) from the append-only bronze streaming tables. Run it from the pipeline, not cell by cell.
-- MAGIC
-- MAGIC | Bronze source | Silver table | Key |
-- MAGIC | --- | --- | --- |
-- MAGIC | `upwind_catalog.bronze.sf_accounts` | `upwind_catalog.silver.sf_accounts` | `account_id` |
-- MAGIC | `upwind_catalog.bronze.sf_transactions` | `upwind_catalog.silver.sf_transactions` | `transaction_id` |
-- MAGIC | `upwind_catalog.bronze.sf_contacts` | `upwind_catalog.silver.sf_contacts` | `contact_id` |
-- MAGIC | `upwind_catalog.bronze.sf_opportunities` | `upwind_catalog.silver.sf_opportunities` | `opportunity_id` |
-- MAGIC | `upwind_catalog.bronze.sf_tickets` | `upwind_catalog.silver.sf_tickets` | `ticket_id` |
-- MAGIC
-- MAGIC **Why**: bronze reads CSVs with `allowOverwrites => true`, so a rewritten file (e.g. the generator re-run) appends all its rows again. Silver upserts them instead.
-- MAGIC
-- MAGIC **How**
-- MAGIC * `KEYS (<id>)`, `SEQUENCE BY _ingested_at`: the most recently loaded version of a key wins. Same columns as bronze; `_ingested_at` / `_source_file` describe the kept version.
-- MAGIC * Each key column is `NOT NULL` with an informational `PRIMARY KEY` (`silver_<table>_pk`). Bronze already fails the update on NULL keys.
-- MAGIC * Column comments match `/Volumes/upwind_catalog/bronze/salesforce/_metadata/table_comments.json`.
-- MAGIC * Rows removed from a CSV are not deleted from silver (no delete signal in the files).

-- COMMAND ----------

-- DBTITLE 1,silver.sf_accounts
-- Silver accounts: one row per account_id. AUTO CDC upserts from the append-only bronze table, keeping the latest version.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.silver.sf_accounts (
  account_id STRING NOT NULL COMMENT 'Unique account key (ACC#####). Joins to account_id in all other sf_ tables.',
  account_name STRING COMMENT 'Company name.',
  industry STRING COMMENT 'Industry of the account (e.g. Technology, Healthcare, Energy).',
  account_type STRING COMMENT 'Relationship type: Customer, Partner or Prospect.',
  country STRING COMMENT 'Headquarters country; determines the currency of transactions and opportunities.',
  employee_count INT COMMENT 'Company headcount (size of the customer organisation, not number of contacts).',
  annual_revenue DOUBLE COMMENT 'Estimated annual company revenue, in the account local currency.',
  website STRING COMMENT 'Company website URL; its domain is used for contact email addresses.',
  created_date DATE COMMENT 'Date the account was created in Salesforce.',
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: bronze load timestamp of the row version kept (latest per key).',
  _source_file STRING COMMENT 'Ingestion metadata: source CSV file of the row version kept.',
  CONSTRAINT silver_sf_accounts_pk PRIMARY KEY (account_id)
)
COMMENT 'Silver Salesforce Accounts: exactly one row per account_id, deduplicated from upwind_catalog.bronze.sf_accounts (latest version by _ingested_at, SCD type 1). Re-loaded or rewritten CSV files update rows instead of duplicating them. Built by the Salesforce Bronze Ingestion (SQL) pipeline.';

CREATE FLOW silver_sf_accounts_dedup AS AUTO CDC INTO upwind_catalog.silver.sf_accounts
FROM STREAM(upwind_catalog.bronze.sf_accounts)
KEYS (account_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- DBTITLE 1,silver.sf_transactions
-- Silver transactions: one row per transaction_id. AUTO CDC upserts from the append-only bronze table, keeping the latest version.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.silver.sf_transactions (
  transaction_id STRING NOT NULL COMMENT 'Unique transaction key (TXN######).',
  account_id STRING COMMENT 'Account the transaction belongs to (FK to sf_accounts.account_id).',
  transaction_date DATE COMMENT 'Date of the transaction; always after the account created_date.',
  amount DECIMAL(18,2) COMMENT 'Transaction amount in the currency column (not converted).',
  currency STRING COMMENT 'ISO currency code (USD, EUR, GBP, CAD, AUD, JPY), derived from the account country.',
  product_type STRING COMMENT 'What was sold: Subscription, License, Services or Support.',
  status STRING COMMENT 'Payment status: Completed, Pending or Refunded.',
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: bronze load timestamp of the row version kept (latest per key).',
  _source_file STRING COMMENT 'Ingestion metadata: source CSV file of the row version kept.',
  CONSTRAINT silver_sf_transactions_pk PRIMARY KEY (transaction_id)
)
COMMENT 'Silver Salesforce Transactions: exactly one row per transaction_id, deduplicated from upwind_catalog.bronze.sf_transactions (latest version by _ingested_at, SCD type 1). Re-loaded or rewritten CSV files update rows instead of duplicating them. Built by the Salesforce Bronze Ingestion (SQL) pipeline.';

CREATE FLOW silver_sf_transactions_dedup AS AUTO CDC INTO upwind_catalog.silver.sf_transactions
FROM STREAM(upwind_catalog.bronze.sf_transactions)
KEYS (transaction_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- DBTITLE 1,silver.sf_contacts
-- Silver contacts: one row per contact_id. AUTO CDC upserts from the append-only bronze table, keeping the latest version.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.silver.sf_contacts (
  contact_id STRING NOT NULL COMMENT 'Unique contact key (EMP######, prefix kept from the former sf_employees table).',
  account_id STRING COMMENT 'Account the contact works for (FK to sf_accounts.account_id).',
  first_name STRING COMMENT 'Contact first name. PII.',
  last_name STRING COMMENT 'Contact last name. PII.',
  email STRING COMMENT 'Contact work email on the account domain. PII.',
  title STRING COMMENT 'Job title (e.g. CISO, VP Engineering, Procurement Specialist).',
  department STRING COMMENT 'Department (Engineering, Finance, Sales, Marketing, IT, Operations, Procurement).',
  phone STRING COMMENT 'Contact phone number. PII.',
  start_date DATE COMMENT 'Date the contact relationship started; on or after the account created_date.',
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: bronze load timestamp of the row version kept (latest per key).',
  _source_file STRING COMMENT 'Ingestion metadata: source CSV file of the row version kept.',
  CONSTRAINT silver_sf_contacts_pk PRIMARY KEY (contact_id)
)
COMMENT 'Silver Salesforce Contacts: exactly one row per contact_id, deduplicated from upwind_catalog.bronze.sf_contacts (latest version by _ingested_at, SCD type 1). Re-loaded or rewritten CSV files update rows instead of duplicating them. Built by the Salesforce Bronze Ingestion (SQL) pipeline.';

CREATE FLOW silver_sf_contacts_dedup AS AUTO CDC INTO upwind_catalog.silver.sf_contacts
FROM STREAM(upwind_catalog.bronze.sf_contacts)
KEYS (contact_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- DBTITLE 1,silver.sf_opportunities
-- Silver opportunities: one row per opportunity_id. AUTO CDC upserts from the append-only bronze table, keeping the latest version.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.silver.sf_opportunities (
  opportunity_id STRING NOT NULL COMMENT 'Unique opportunity key (OPP######).',
  account_id STRING COMMENT 'Account the opportunity belongs to (FK to sf_accounts.account_id).',
  opportunity_name STRING COMMENT 'Opportunity name: account - Upwind product (type).',
  type STRING COMMENT 'Deal type: New Business, Expansion or Renewal.',
  stage STRING COMMENT 'Sales stage: Prospecting, Qualification, Needs Analysis, Proposal, Negotiation, Closed Won, Closed Lost.',
  probability INT COMMENT 'Win probability in percent (0-100), set by stage.',
  amount DECIMAL(18,2) COMMENT 'Deal amount in the currency column (not converted).',
  currency STRING COMMENT 'ISO currency code derived from the account country.',
  lead_source STRING COMMENT 'Where the deal came from: Web, Partner Referral, Event, Outbound, Inbound.',
  primary_contact_id STRING COMMENT 'Main customer contact (FK to sf_contacts.contact_id, same account).',
  created_date DATE COMMENT 'Date the opportunity was created.',
  close_date DATE COMMENT 'Actual close date for closed deals, expected close date for open deals.',
  is_closed BOOLEAN COMMENT 'True when stage is Closed Won or Closed Lost.',
  is_won BOOLEAN COMMENT 'True when stage is Closed Won.',
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: bronze load timestamp of the row version kept (latest per key).',
  _source_file STRING COMMENT 'Ingestion metadata: source CSV file of the row version kept.',
  CONSTRAINT silver_sf_opportunities_pk PRIMARY KEY (opportunity_id)
)
COMMENT 'Silver Salesforce Opportunities: exactly one row per opportunity_id, deduplicated from upwind_catalog.bronze.sf_opportunities (latest version by _ingested_at, SCD type 1). Re-loaded or rewritten CSV files update rows instead of duplicating them. Built by the Salesforce Bronze Ingestion (SQL) pipeline.';

CREATE FLOW silver_sf_opportunities_dedup AS AUTO CDC INTO upwind_catalog.silver.sf_opportunities
FROM STREAM(upwind_catalog.bronze.sf_opportunities)
KEYS (opportunity_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- DBTITLE 1,silver.sf_tickets
-- Silver tickets: one row per ticket_id. AUTO CDC upserts from the append-only bronze table, keeping the latest version.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.silver.sf_tickets (
  ticket_id STRING NOT NULL COMMENT 'Unique support ticket key (TKT######). Primary key.',
  account_id STRING COMMENT 'Account that opened the ticket (FK to sf_accounts.account_id).',
  reporter_contact_id STRING COMMENT 'Contact who reported the ticket (FK to sf_contacts.contact_id, same account).',
  created_at TIMESTAMP COMMENT 'Timestamp the ticket was opened.',
  first_response_at TIMESTAMP COMMENT 'Timestamp of the first support response; NULL if not answered yet.',
  resolved_at TIMESTAMP COMMENT 'Timestamp the ticket was resolved; NULL while open.',
  status STRING COMMENT 'Lifecycle status: New, In Progress, Waiting on Customer, Resolved, Closed.',
  priority STRING COMMENT 'Business priority: P1 - Critical, P2 - High, P3 - Medium, P4 - Low. Drives the SLA target.',
  ticket_type STRING COMMENT 'Bug, Incident, Configuration, Question or Feature Request.',
  product_module STRING COMMENT 'Upwind module affected: Runtime Sensor, CSPM, Vulnerability Management, KSPM, Cloud Detection & Response, Compliance & Reporting, API Security, Integrations, Console & Access.',
  subject STRING COMMENT 'Short ticket title describing the problem.',
  description STRING COMMENT 'Full description of the problem as in the support case body: symptom and error/log message (e.g. CrashLoopBackOff, AccessDenied, HTTP 403), environment, Upwind version and affected assets, plus current status / resolution and the engineering bug key for bugs. Cloud is in cloud_provider.',
  cloud_provider STRING COMMENT 'Customer cloud involved: AWS, Azure, GCP, Multi-cloud, or N/A for console issues.',
  environment STRING COMMENT 'Customer environment affected: Production, Staging or Development.',
  channel STRING COMMENT 'How the ticket was opened: Portal, Email, Slack Connect, In-app, Phone.',
  product_version STRING COMMENT 'Upwind platform / sensor version reported by the customer.',
  affected_assets INT COMMENT 'Number of customer cloud assets (hosts, containers, resources) impacted; a measure of impact.',
  root_cause STRING COMMENT 'Root cause after investigation (Product Bug, Configuration, Customer Environment, Third-party Integration, etc.); Under Investigation while open.',
  is_bug BOOLEAN COMMENT 'True when the ticket is a confirmed product bug (type Bug, or an Incident with root cause Product Bug).',
  bug_id STRING COMMENT 'Engineering bug reference (UPW-####) when is_bug is true.',
  bug_severity STRING COMMENT 'Bug severity S1-S4, mapped from priority, when is_bug is true.',
  support_tier STRING COMMENT 'Support tier handling the ticket: Tier 1, Tier 2 or Tier 3 (escalated).',
  escalated BOOLEAN COMMENT 'True when the ticket was escalated (all P1s, reopened tickets, some P2s).',
  reopen_count INT COMMENT 'Number of times the ticket was reopened after being resolved.',
  sla_first_response_minutes INT COMMENT 'First-response SLA target in minutes for the priority (P1 30, P2 120, P3 480, P4 1440).',
  first_response_minutes INT COMMENT 'Actual minutes from creation to first response; NULL if not answered yet.',
  sla_breached BOOLEAN COMMENT 'True when first_response_minutes exceeded sla_first_response_minutes.',
  resolution_hours DOUBLE COMMENT 'Hours from creation to resolution; NULL while open.',
  csat_score INT COMMENT 'Customer satisfaction with this ticket handling, 1 (very dissatisfied) to 5 (very satisfied). Only on resolved tickets with a survey response.',
  nps_score INT COMMENT 'Net Promoter answer, 0-10: how likely the customer is to recommend Upwind. 9-10 promoter, 7-8 passive, 0-6 detractor.',
  sentiment STRING COMMENT 'Feedback sentiment derived from csat_score: Positive (4-5), Neutral (3), Negative (1-2).',
  feedback_comment STRING COMMENT 'Free-text customer feedback from the satisfaction survey.',
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: bronze load timestamp of the row version kept (latest per key).',
  _source_file STRING COMMENT 'Ingestion metadata: source CSV file of the row version kept.',
  CONSTRAINT silver_sf_tickets_pk PRIMARY KEY (ticket_id)
)
COMMENT 'Silver Salesforce Tickets: exactly one row per ticket_id, deduplicated from upwind_catalog.bronze.sf_tickets (latest version by _ingested_at, SCD type 1). Re-loaded or rewritten CSV files update rows instead of duplicating them. Built by the Salesforce Bronze Ingestion (SQL) pipeline.';

CREATE FLOW silver_sf_tickets_dedup AS AUTO CDC INTO upwind_catalog.silver.sf_tickets
FROM STREAM(upwind_catalog.bronze.sf_tickets)
KEYS (ticket_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;