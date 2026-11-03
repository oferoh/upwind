-- Databricks notebook source
-- DBTITLE 1,Overview
-- MAGIC %md
-- MAGIC # Salesforce Bronze Ingestion
-- MAGIC
-- MAGIC Source code of the Lakeflow Spark Declarative Pipeline **Salesforce Bronze Ingestion (SQL)**. It loads the Upwind Salesforce sample CSVs from the volume `/Volumes/upwind_catalog/bronze/salesforce` into streaming tables in `upwind_catalog.bronze`. Run it from the pipeline, not cell by cell.
-- MAGIC
-- MAGIC | Source folder | Target streaming table |
-- MAGIC | --- | --- |
-- MAGIC | `accounts/` | `upwind_catalog.bronze.sf_accounts` |
-- MAGIC | `transactions/` | `upwind_catalog.bronze.sf_transactions` |
-- MAGIC | `contacts/` | `upwind_catalog.bronze.sf_contacts` |
-- MAGIC | `opportunities/` | `upwind_catalog.bronze.sf_opportunities` |
-- MAGIC | `tickets/` | `upwind_catalog.bronze.sf_tickets` |
-- MAGIC
-- MAGIC `employees/` (legacy, replaced by `contacts/`) and `_metadata/` (config, not data) are not loaded.
-- MAGIC
-- MAGIC **How it loads**
-- MAGIC * Incremental Auto Loader ingestion with `STREAM read_files(...)`: each update processes only new files. `allowOverwrites => true` also re-reads files that were rewritten, which appends their rows again, so dedupe downstream (silver) on the primary key.
-- MAGIC * Explicit schemas (same column names and types as the former loader notebook) with `mode => 'FAILFAST'`, so a malformed or mistyped row fails the update instead of loading NULLs.
-- MAGIC * Metadata columns: `_ingested_at` (load timestamp) and `_source_file` (`_metadata.file_path`).
-- MAGIC * Table and column comments are copied from `/Volumes/upwind_catalog/bronze/salesforce/_metadata/table_comments.json`. If that file changes, update the comments here too.
-- MAGIC * `sf_tickets` has `ticket_id NOT NULL` and primary key `sf_tickets_pk` (informational, not enforced).
-- MAGIC
-- MAGIC **Data quality (expectations)**
-- MAGIC * `ON VIOLATION FAIL UPDATE`: missing primary id, and missing `account_id` on child entities.
-- MAGIC * Warn only (counted in the pipeline Data quality tab): formats, allowed values, ranges, date and status consistency rules from the former notebook.
-- MAGIC * Tickets: future-dated tickets and NULL first responses are legitimate and never fail the update.

-- COMMAND ----------

-- DBTITLE 1,sf_accounts
-- Bronze: accounts CSVs from /Volumes/upwind_catalog/bronze/salesforce/accounts/ (incremental Auto Loader ingestion).
-- Column/table comments copied from /Volumes/upwind_catalog/bronze/salesforce/_metadata/table_comments.json.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.bronze.sf_accounts (
  account_id STRING COMMENT 'Unique account key (ACC#####). Joins to account_id in all other sf_ tables.',
  account_name STRING COMMENT 'Company name.',
  industry STRING COMMENT 'Industry of the account (e.g. Technology, Healthcare, Energy).',
  account_type STRING COMMENT 'Relationship type: Customer, Partner or Prospect.',
  country STRING COMMENT 'Headquarters country; determines the currency of transactions and opportunities.',
  employee_count INT COMMENT 'Company headcount (size of the customer organisation, not number of contacts).',
  annual_revenue DOUBLE COMMENT 'Estimated annual company revenue, in the account local currency.',
  website STRING COMMENT 'Company website URL; its domain is used for contact email addresses.',
  created_date DATE COMMENT 'Date the account was created in Salesforce.',
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: timestamp when the row was loaded into bronze.',
  _source_file STRING COMMENT 'Ingestion metadata: full path of the source CSV file in the salesforce volume.',
  CONSTRAINT account_id_not_null EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT account_id_format EXPECT (account_id RLIKE '^ACC[0-9]+$'),
  CONSTRAINT account_name_not_null EXPECT (account_name IS NOT NULL AND trim(account_name) <> ''),
  CONSTRAINT created_date_not_null EXPECT (created_date IS NOT NULL),
  CONSTRAINT created_date_not_future EXPECT (created_date <= current_date()),
  CONSTRAINT account_type_allowed EXPECT (account_type IN ('Customer', 'Partner', 'Prospect')),
  CONSTRAINT employee_count_positive EXPECT (employee_count > 0),
  CONSTRAINT annual_revenue_non_negative EXPECT (annual_revenue >= 0),
  CONSTRAINT website_valid_format EXPECT (website RLIKE '^https?://[^ ]+\\.[a-z]{2,}$')
)
COMMENT 'Bronze Salesforce Accounts: one row per Upwind customer, partner or prospect company. Source: generated Upwind sample data (Generate Salesforce Sample Data notebook), loaded by Load Salesforce CSVs to Bronze (full overwrite each run). Grain: account_id.'
AS SELECT
  account_id,
  account_name,
  industry,
  account_type,
  country,
  employee_count,
  annual_revenue,
  website,
  created_date,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/upwind_catalog/bronze/salesforce/accounts/',
  format => 'csv',
  header => true,
  mode => 'FAILFAST',
  allowOverwrites => true,
  schema => 'account_id STRING, account_name STRING, industry STRING, account_type STRING, country STRING, employee_count INT, annual_revenue DOUBLE, website STRING, created_date DATE'
);

-- COMMAND ----------

-- DBTITLE 1,sf_transactions
-- Bronze: transactions CSVs from /Volumes/upwind_catalog/bronze/salesforce/transactions/ (incremental Auto Loader ingestion).
-- Column/table comments copied from /Volumes/upwind_catalog/bronze/salesforce/_metadata/table_comments.json.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.bronze.sf_transactions (
  transaction_id STRING COMMENT 'Unique transaction key (TXN######).',
  account_id STRING COMMENT 'Account the transaction belongs to (FK to sf_accounts.account_id).',
  transaction_date DATE COMMENT 'Date of the transaction; always after the account created_date.',
  amount DECIMAL(18,2) COMMENT 'Transaction amount in the currency column (not converted).',
  currency STRING COMMENT 'ISO currency code (USD, EUR, GBP, CAD, AUD, JPY), derived from the account country.',
  product_type STRING COMMENT 'What was sold: Subscription, License, Services or Support.',
  status STRING COMMENT 'Payment status: Completed, Pending or Refunded.',
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: timestamp when the row was loaded into bronze.',
  _source_file STRING COMMENT 'Ingestion metadata: full path of the source CSV file in the salesforce volume.',
  CONSTRAINT transaction_id_not_null EXPECT (transaction_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT account_id_not_null EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT transaction_id_format EXPECT (transaction_id RLIKE '^TXN[0-9]+$'),
  CONSTRAINT transaction_date_not_null EXPECT (transaction_date IS NOT NULL),
  CONSTRAINT transaction_date_not_future EXPECT (transaction_date <= current_date()),
  CONSTRAINT amount_positive EXPECT (amount > 0),
  CONSTRAINT currency_allowed EXPECT (currency IN ('USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY')),
  CONSTRAINT product_type_allowed EXPECT (product_type IN ('Subscription', 'License', 'Services', 'Support')),
  CONSTRAINT status_allowed EXPECT (status IN ('Completed', 'Pending', 'Refunded'))
)
COMMENT 'Bronze Salesforce Transactions: billed transactions per account (subscriptions, licenses, services, support). Amounts are in the transaction currency, not converted. Source: generated Upwind sample data (Generate Salesforce Sample Data notebook), loaded by Load Salesforce CSVs to Bronze (full overwrite each run). Grain: transaction_id.'
AS SELECT
  transaction_id,
  account_id,
  transaction_date,
  amount,
  currency,
  product_type,
  status,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/upwind_catalog/bronze/salesforce/transactions/',
  format => 'csv',
  header => true,
  mode => 'FAILFAST',
  allowOverwrites => true,
  schema => 'transaction_id STRING, account_id STRING, transaction_date DATE, amount DECIMAL(18,2), currency STRING, product_type STRING, status STRING'
);

-- COMMAND ----------

-- DBTITLE 1,sf_contacts
-- Bronze: contacts CSVs from /Volumes/upwind_catalog/bronze/salesforce/contacts/ (incremental Auto Loader ingestion).
-- Column/table comments copied from /Volumes/upwind_catalog/bronze/salesforce/_metadata/table_comments.json.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.bronze.sf_contacts (
  contact_id STRING COMMENT 'Unique contact key (EMP######, prefix kept from the former sf_employees table).',
  account_id STRING COMMENT 'Account the contact works for (FK to sf_accounts.account_id).',
  first_name STRING COMMENT 'Contact first name. PII.',
  last_name STRING COMMENT 'Contact last name. PII.',
  email STRING COMMENT 'Contact work email on the account domain. PII.',
  title STRING COMMENT 'Job title (e.g. CISO, VP Engineering, Procurement Specialist).',
  department STRING COMMENT 'Department (Engineering, Finance, Sales, Marketing, IT, Operations, Procurement).',
  phone STRING COMMENT 'Contact phone number. PII.',
  start_date DATE COMMENT 'Date the contact relationship started; on or after the account created_date.',
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: timestamp when the row was loaded into bronze.',
  _source_file STRING COMMENT 'Ingestion metadata: full path of the source CSV file in the salesforce volume.',
  CONSTRAINT contact_id_not_null EXPECT (contact_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT account_id_not_null EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT contact_id_format EXPECT (contact_id RLIKE '^EMP[0-9]+$'),
  CONSTRAINT last_name_not_null EXPECT (last_name IS NOT NULL AND trim(last_name) <> ''),
  CONSTRAINT email_valid_format EXPECT (email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'),
  CONSTRAINT start_date_not_future EXPECT (start_date <= current_date())
)
COMMENT 'Bronze Salesforce Contacts: people at each customer account (2-5 per account). Referenced by opportunities (primary_contact_id) and tickets (reporter_contact_id). Source: generated Upwind sample data (Generate Salesforce Sample Data notebook), loaded by Load Salesforce CSVs to Bronze (full overwrite each run). Grain: contact_id. May contain PII (name, email, phone).'
AS SELECT
  contact_id,
  account_id,
  first_name,
  last_name,
  email,
  title,
  department,
  phone,
  start_date,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/upwind_catalog/bronze/salesforce/contacts/',
  format => 'csv',
  header => true,
  mode => 'FAILFAST',
  allowOverwrites => true,
  schema => 'contact_id STRING, account_id STRING, first_name STRING, last_name STRING, email STRING, title STRING, department STRING, phone STRING, start_date DATE'
);

-- COMMAND ----------

-- DBTITLE 1,sf_opportunities
-- Bronze: opportunities CSVs from /Volumes/upwind_catalog/bronze/salesforce/opportunities/ (incremental Auto Loader ingestion).
-- Column/table comments copied from /Volumes/upwind_catalog/bronze/salesforce/_metadata/table_comments.json.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.bronze.sf_opportunities (
  opportunity_id STRING COMMENT 'Unique opportunity key (OPP######).',
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
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: timestamp when the row was loaded into bronze.',
  _source_file STRING COMMENT 'Ingestion metadata: full path of the source CSV file in the salesforce volume.',
  CONSTRAINT opportunity_id_not_null EXPECT (opportunity_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT account_id_not_null EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT opportunity_id_format EXPECT (opportunity_id RLIKE '^OPP[0-9]+$'),
  CONSTRAINT close_date_not_null EXPECT (close_date IS NOT NULL),
  CONSTRAINT stage_allowed EXPECT (stage IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost')),
  CONSTRAINT type_allowed EXPECT (type IN ('New Business', 'Expansion', 'Renewal')),
  CONSTRAINT currency_allowed EXPECT (currency IN ('USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY')),
  CONSTRAINT probability_in_range EXPECT (probability BETWEEN 0 AND 100),
  CONSTRAINT amount_positive EXPECT (amount > 0),
  CONSTRAINT is_closed_consistent EXPECT (is_closed = (stage IN ('Closed Won', 'Closed Lost'))),
  CONSTRAINT is_won_consistent EXPECT (is_won = (stage = 'Closed Won')),
  CONSTRAINT close_after_created EXPECT (coalesce(close_date >= created_date, true))
)
COMMENT 'Bronze Salesforce Opportunities: Upwind sales pipeline per account (new business, expansion, renewal). Amounts are in the opportunity currency, not converted. Source: generated Upwind sample data (Generate Salesforce Sample Data notebook), loaded by Load Salesforce CSVs to Bronze (full overwrite each run). Grain: opportunity_id.'
AS SELECT
  opportunity_id,
  account_id,
  opportunity_name,
  type,
  stage,
  probability,
  amount,
  currency,
  lead_source,
  primary_contact_id,
  created_date,
  close_date,
  is_closed,
  is_won,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/upwind_catalog/bronze/salesforce/opportunities/',
  format => 'csv',
  header => true,
  mode => 'FAILFAST',
  allowOverwrites => true,
  schema => 'opportunity_id STRING, account_id STRING, opportunity_name STRING, type STRING, stage STRING, probability INT, amount DECIMAL(18,2), currency STRING, lead_source STRING, primary_contact_id STRING, created_date DATE, close_date DATE, is_closed BOOLEAN, is_won BOOLEAN'
);

-- COMMAND ----------

-- DBTITLE 1,sf_tickets
-- Bronze: tickets CSVs from /Volumes/upwind_catalog/bronze/salesforce/tickets/ (incremental Auto Loader ingestion).
-- Column/table comments copied from /Volumes/upwind_catalog/bronze/salesforce/_metadata/table_comments.json.
-- Future-dated tickets and NULL first responses are legitimate: those checks only warn.
CREATE OR REFRESH STREAMING TABLE upwind_catalog.bronze.sf_tickets (
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
  _ingested_at TIMESTAMP COMMENT 'Ingestion metadata: timestamp when the row was loaded into bronze.',
  _source_file STRING COMMENT 'Ingestion metadata: full path of the source CSV file in the salesforce volume.',
  CONSTRAINT ticket_id_not_null EXPECT (ticket_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT account_id_not_null EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT ticket_id_format EXPECT (ticket_id RLIKE '^TKT[0-9]+$'),
  CONSTRAINT created_at_not_null EXPECT (created_at IS NOT NULL),
  CONSTRAINT description_present EXPECT (length(trim(description)) >= 20),
  CONSTRAINT created_at_not_future EXPECT (created_at <= current_timestamp()),
  CONSTRAINT status_allowed EXPECT (status IN ('New', 'In Progress', 'Waiting on Customer', 'Resolved', 'Closed')),
  CONSTRAINT priority_allowed EXPECT (priority IN ('P1 - Critical', 'P2 - High', 'P3 - Medium', 'P4 - Low')),
  CONSTRAINT ticket_type_allowed EXPECT (ticket_type IN ('Bug', 'Incident', 'Configuration', 'Question', 'Feature Request')),
  CONSTRAINT product_module_allowed EXPECT (product_module IN ('Runtime Sensor', 'CSPM', 'Vulnerability Management', 'KSPM', 'Cloud Detection & Response', 'Compliance & Reporting', 'API Security', 'Integrations', 'Console & Access')),
  CONSTRAINT response_after_created EXPECT (first_response_at IS NULL OR first_response_at >= created_at),
  CONSTRAINT resolved_after_created EXPECT (resolved_at IS NULL OR resolved_at >= created_at),
  CONSTRAINT resolved_has_timestamp EXPECT (NOT (status IN ('Resolved', 'Closed') AND resolved_at IS NULL)),
  CONSTRAINT open_has_no_resolution EXPECT (status IN ('Resolved', 'Closed') OR resolved_at IS NULL),
  CONSTRAINT sla_consistent EXPECT (first_response_minutes IS NULL OR coalesce(sla_breached = (first_response_minutes > sla_first_response_minutes), false)),
  CONSTRAINT csat_in_range EXPECT (csat_score IS NULL OR csat_score BETWEEN 1 AND 5),
  CONSTRAINT nps_in_range EXPECT (nps_score IS NULL OR nps_score BETWEEN 0 AND 10),
  CONSTRAINT feedback_only_when_resolved EXPECT (csat_score IS NULL OR status IN ('Resolved', 'Closed')),
  CONSTRAINT bug_has_id EXPECT (NOT coalesce(is_bug, false) OR bug_id IS NOT NULL),
  CONSTRAINT reopen_count_non_negative EXPECT (reopen_count IS NULL OR reopen_count >= 0),
  CONSTRAINT sf_tickets_pk PRIMARY KEY (ticket_id)
)
COMMENT 'Bronze Salesforce Support Tickets: Upwind cloud-security support cases (bugs, incidents, configuration, questions, feature requests) with SLA tracking and customer feedback (CSAT/NPS). Used for product stability and customer satisfaction analysis. Source: generated Upwind sample data (Generate Salesforce Sample Data notebook), loaded by Load Salesforce CSVs to Bronze (full overwrite each run). Grain: ticket_id. Some tickets are intentionally future-dated (open, no response yet).'
AS SELECT
  ticket_id,
  account_id,
  reporter_contact_id,
  created_at,
  first_response_at,
  resolved_at,
  status,
  priority,
  ticket_type,
  product_module,
  subject,
  description,
  cloud_provider,
  environment,
  channel,
  product_version,
  affected_assets,
  root_cause,
  is_bug,
  bug_id,
  bug_severity,
  support_tier,
  escalated,
  reopen_count,
  sla_first_response_minutes,
  first_response_minutes,
  sla_breached,
  resolution_hours,
  csat_score,
  nps_score,
  sentiment,
  feedback_comment,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/upwind_catalog/bronze/salesforce/tickets/',
  format => 'csv',
  header => true,
  mode => 'FAILFAST',
  allowOverwrites => true,
  schema => 'ticket_id STRING, account_id STRING, reporter_contact_id STRING, created_at TIMESTAMP, first_response_at TIMESTAMP, resolved_at TIMESTAMP, status STRING, priority STRING, ticket_type STRING, product_module STRING, subject STRING, description STRING, cloud_provider STRING, environment STRING, channel STRING, product_version STRING, affected_assets INT, root_cause STRING, is_bug BOOLEAN, bug_id STRING, bug_severity STRING, support_tier STRING, escalated BOOLEAN, reopen_count INT, sla_first_response_minutes INT, first_response_minutes INT, sla_breached BOOLEAN, resolution_hours DOUBLE, csat_score INT, nps_score INT, sentiment STRING, feedback_comment STRING'
);