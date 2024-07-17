-- Load raw data in SQL server as tables

-- Check for duplicate values
SELECT marketing_touchpoint_id, channel_name, contact_id, marketing_touchpoint_date, COUNT(*)
FROM [dbo].[de_hw_marketing_data]
GROUP BY marketing_touchpoint_id, channel_name, contact_id, marketing_touchpoint_date
HAVING COUNT(*) > 1;

SELECT sales_touchpoint_id, channel_name, contact_id, sales_touchpoint_date, COUNT(*)
FROM [dbo].[de_hw_sales_outreach_data]
GROUP BY sales_touchpoint_id, channel_name, contact_id, sales_touchpoint_date
HAVING COUNT(*) > 1;

SELECT contact_id, account_id, COUNT(*)
FROM [dbo].[contact_data]
GROUP BY contact_id, account_id
HAVING COUNT(*) > 1;

SELECT opportunity_id, account_id, pipeline_amount, opportunity_created_date, sales_segment, COUNT(*)
FROM [dbo].[de_hw_opportunity_data]
GROUP BY opportunity_id, account_id, pipeline_amount, opportunity_created_date, sales_segment
HAVING COUNT(*) > 1

-- Create required tables with correct data types of columns
/*
CREATE TABLE marketing_data (
    marketing_touchpoint_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    contact_id VARCHAR(255),
    marketing_touchpoint_date DATETIME
);

CREATE TABLE sales_outreach_data (
    sales_touchpoint_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    contact_id VARCHAR(255),
    sales_touchpoint_date DATETIME
);

CREATE TABLE contact_data (
    contact_id VARCHAR(255) PRIMARY KEY,
    account_id VARCHAR(255)
);

CREATE TABLE opportunity_data (
    opportunity_id VARCHAR(255) PRIMARY KEY,
    account_id VARCHAR(255),
    pipeline_amount FLOAT,
    opportunity_created_date DATETIME,
    sales_segment VARCHAR(255)
);
*/

-- Data importing and pre-processing
INSERT INTO marketing_data (marketing_touchpoint_id, channel_name, contact_id, marketing_touchpoint_date)
SELECT 
    marketing_touchpoint_id,
    channel_name,
    contact_id,
    CONVERT(DATETIME, REPLACE(REPLACE(marketing_touchpoint_date, 'T', ' '), 'Z', ''), 120)
FROM [dbo].[de_hw_marketing_data];

INSERT INTO sales_outreach_data (sales_touchpoint_id, channel_name, contact_id, sales_touchpoint_date)
SELECT 
    sales_touchpoint_id,
    channel_name,
    contact_id,
    CONVERT(DATETIME, REPLACE(REPLACE(sales_touchpoint_date, 'T', ' '), 'Z', ''), 120)
FROM [dbo].[de_hw_sales_outreach_data];

INSERT INTO contact_data (contact_id, account_id)
SELECT 
    contact_id,
    account_id
FROM [dbo].[de_hw_contact_data][dbo];

INSERT INTO dbo.opportunity_data (opportunity_id, account_id, pipeline_amount, opportunity_created_date, sales_segment)
SELECT 
    opportunity_id,
    account_id,
    pipeline_amount,
    CONVERT(DATETIME, REPLACE(REPLACE(Opportunity_Created_Date, 'T', ' '), 'Z', ''), 120),
	sales_segment
FROM [dbo].[de_hw_opportunity_data];

-------------------- DATA IMPORTING AND PRE-PROCESSING COMPLETE --------------------
------------------------------------------------------------------------------------

-- Create a view with all touchpoints from Marketing and Sales
CREATE VIEW all_touchpoints AS
SELECT
    marketing_touchpoint_id AS touchpoint_id,
    'marketing' AS touchpoint_type,
    channel_name,
    contact_id,
    marketing_touchpoint_date AS touchpoint_date
FROM marketing_data
UNION ALL
SELECT
    sales_touchpoint_id AS touchpoint_id,
    'sales' AS touchpoint_type,
    channel_name,
    contact_id,
    sales_touchpoint_date AS touchpoint_date
FROM sales_outreach_data;

-- Data table with all touchpoints, opportunity interaction and pipeline amount
SELECT
    o.opportunity_id,
    o.account_id,
    o.pipeline_amount,
    o.opportunity_created_date,
	o.sales_segment,
    t.touchpoint_id,
    t.touchpoint_type,
    t.channel_name,
    t.contact_id,
    t.touchpoint_date
INTO opportunity_touchpoints
FROM opportunity_data o
JOIN contact_data c ON o.account_id = c.account_id
JOIN all_touchpoints t ON c.contact_id = t.contact_id
WHERE t.touchpoint_date BETWEEN DATEADD(DAY, -90, o.opportunity_created_date) AND o.opportunity_created_date;

CREATE VIEW first_touchpoints AS
SELECT
    opportunity_id,
    MIN(touchpoint_date) AS first_touchpoint_date
FROM opportunity_touchpoints
GROUP BY opportunity_id;

-- Create a table with all the details about first touchpoint
SELECT
    ot.opportunity_id,
    ot.account_id,
    ot.pipeline_amount,
    ot.opportunity_created_date,
	ot.sales_segment,
    ot.touchpoint_id,
    ot.touchpoint_type,
    ot.channel_name,
    ot.contact_id,
    ot.touchpoint_date
INTO first_touchpoint_details
FROM opportunity_touchpoints ot
JOIN first_touchpoints ft ON ot.opportunity_id = ft.opportunity_id AND ot.touchpoint_date = ft.first_touchpoint_date;

-- Total Pipeline Sourced by Each Channel
SELECT
channel_name,
SUM(pipeline_amount) AS total_pipeline
FROM first_touchpoint_details
GROUP BY channel_name
ORDER BY total_pipeline DESC;

-- Pipeline Sourced by Each Channel by Sales Segment
SELECT
    sales_segment,
    channel_name,
    SUM(pipeline_amount) AS total_pipeline
FROM first_touchpoint_details
GROUP BY sales_segment, channel_name
ORDER BY total_pipeline DESC;

-------------------- DATA VALIDATION --------------------
-- Check for duplicate touchpoints
SELECT touchpoint_id, COUNT(*)
FROM all_touchpoints
GROUP BY touchpoint_id
HAVING COUNT(*) > 1;

-- Check for touchpoints outside the 90-day window
SELECT *
FROM opportunity_touchpoints
WHERE touchpoint_date NOT BETWEEN DATEADD(DAY, -90, opportunity_created_date) AND opportunity_created_date;

------------------------------------------------------------------------------------------------------------------------------------

-------------------- DIFFERENT ATTRIBUTIN MODELS --------------------

-- Last-Touch Attribution
CREATE VIEW last_touchpoints AS
SELECT
    ot.opportunity_id,
    ot.account_id,
    ot.pipeline_amount,
    ot.opportunity_created_date,
	ot.sales_segment,
    ot.touchpoint_id,
    ot.touchpoint_type,
    ot.channel_name,
    ot.contact_id,
    ot.touchpoint_date
FROM (
    SELECT
        o.opportunity_id,
        o.account_id,
        o.pipeline_amount,
        o.opportunity_created_date,
		o.sales_segment,
        t.touchpoint_id,
        t.touchpoint_type,
        t.channel_name,
        t.contact_id,
        t.touchpoint_date,
        ROW_NUMBER() OVER (PARTITION BY o.opportunity_id ORDER BY t.touchpoint_date DESC) AS rn
    FROM opportunity_data o
    JOIN contact_data c ON o.account_id = c.account_id
    JOIN all_touchpoints t ON c.contact_id = t.contact_id
    WHERE t.touchpoint_date BETWEEN DATEADD(DAY, -90, o.opportunity_created_date) AND o.opportunity_created_date
) ot
WHERE ot.rn = 1;

SELECT			-- Total Pipeline Sourced by Each Channel
    channel_name,
    SUM(pipeline_amount) AS total_pipeline
FROM last_touchpoints
GROUP BY channel_name
ORDER BY total_pipeline DESC;

SELECT			-- Pipeline Sourced by Each Channel by Sales Segment
    sales_segment,
    channel_name,
    SUM(pipeline_amount) AS total_pipeline
FROM last_touchpoints
GROUP BY sales_segment, channel_name
ORDER BY sales_segment, total_pipeline DESC;

-- Linear Attribution
CREATE VIEW relevant_touchpoints AS
SELECT
    o.opportunity_id,
    o.account_id,
    o.pipeline_amount,
    o.opportunity_created_date,
	o.sales_segment,
    t.touchpoint_id,
    t.touchpoint_type,
    t.channel_name,
    t.contact_id,
    t.touchpoint_date
FROM opportunity_data o
JOIN contact_data c ON o.account_id = c.account_id
JOIN all_touchpoints t ON c.contact_id = t.contact_id
WHERE t.touchpoint_date BETWEEN DATEADD(DAY, -90, o.opportunity_created_date) AND o.opportunity_created_date;

CREATE VIEW touchpoint_counts AS
SELECT
    opportunity_id,
    COUNT(touchpoint_id) AS touchpoint_count
FROM relevant_touchpoints
GROUP BY opportunity_id;

CREATE VIEW linear_attribution AS
SELECT
    rt.opportunity_id,
    rt.account_id,
    rt.pipeline_amount / tc.touchpoint_count AS attributed_value,
	rt.sales_segment,
    rt.touchpoint_id,
    rt.touchpoint_type,
    rt.channel_name,
    rt.contact_id,
    rt.touchpoint_date
FROM relevant_touchpoints rt
JOIN touchpoint_counts tc ON rt.opportunity_id = tc.opportunity_id;

SELECT			-- Total Pipeline Sourced by Each Channel
    channel_name,
    SUM(attributed_value) AS total_pipeline
FROM linear_attribution
GROUP BY channel_name
ORDER BY total_pipeline DESC;

SELECT			-- Pipeline Sourced by Each Channel by Sales Segment
    sales_segment,
    channel_name,
    SUM(attributed_value) AS total_pipeline
FROM linear_attribution
GROUP BY sales_segment, channel_name
ORDER BY sales_segment, total_pipeline DESC;
