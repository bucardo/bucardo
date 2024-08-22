# Schema Migration Steps

  

This document outlines the necessary steps to perform schema migration using Bucardo. It includes steps for preparing the code, installing and configuring Bucardo, and adding databases and tables.

  

## Table of Contents

  

1. [Bucardo Installation Prerequisites](#1-bucardo-installation-prerequisites)

2. [Preparing the Bucardo Code](#2-preparing-the-bucardo-code)

3. [Custom Bucardo Installation](#3-custom-bucardo-installation)

4. [Bucardo Sync Setup](#4bucardo-sync-setup)

5. [Tenant Switching](#5tenant-switching)

  

## 1. Bucardo Installation Prerequisites

  

1.1 Install PostgreSQL and its development tools:

  

```bash
sudo apt update

sudo apt install postgresql libpq-dev postgresql-server-dev-16 postgresql-client-16
```

  

1.2 Set up the PostgreSQL user as sudo User:

  

```bash
usermod -aG sudo postgres
```

  

1.3 Set the password for the postgres user as "postgres":

```bash
passwd postgres
```

1.4 Then create a home directory for postgres  and update it’s ownership to postgres

```bash
sudo mkdir /home/postgres
cd /home
chown postgres:postgres postgres
```

1.5 Change the Authentication method for bucardo Postgres DB

switch to postgres user & go to pg_hba file directory
```bash
su postgres
cd /etc/postgresql/16/main
```
  Edit the pg_hba.conf file on the above directory
  >Change all the values under & above method column to trust
  ``` bash
  sudo nano pg_hba.conf
  ```

>Demo file fomat
```bash
# Database administrative login by Unix domain socket

local all postgres trust

# TYPE DATABASE USER ADDRESS METHOD

# "local" is for Unix domain socket connections only

local all all trust

# IPv4 local connections:

host all all 127.0.0.1/32 trust

# IPv6 local connections:

host all all  ::1/128 trust

# Allow replication connections from localhost, by a user with the

# replication privilege.

local replication all trust

host replication all 127.0.0.1/32 trust

host replication all ::1/128 trust
```

Restart the database to apply this configuration 
>It will ask password use the "postgres" password
```bash
systemctl restart postgresql
```

Login to postgresql and check the perl extension exist
> mostly it will not be installed (0 Rows)
```bash
psql
```

```SQL
select * from pg_available_extensions where name like '%perl%';

\q
```

Installing perl extension for postgres

```bash
sudo apt-get install postgresql-plperl-16
```
Login to postgresql and check the query and create perl extension
```SQL
select * from pg_available_extensions where name like '%perl%';

CREATE EXTENSION plperl;

CREATE EXTENSION plperlu;

\q
```
  Install perl libraries which are needed for bucardo installation
  ```bash
sudo apt-get install libdbix-safe-perl

sudo apt-get install libdbd-pg-perl libencode-locale-perl libpod-parser-perl
  ```

Create the below Directories for bucardo
> first one is for logging and another one is for PID file
```bash
 cd /var/log/     
 sudo  mkdir bucardo    
 chown  postgres:postgres bucardo   
 cd /var/run   
 sudo mkdir bucardo
 chown  postgres:postgres bucardo
```
  

## 2. Preparing the Bucardo Code

  

2.1 Modify the bucardo.schema file:

 > Use your own code editor or git hub's editor to change the following things

> Locate and change the project ID at line 2426: Change this to your desired project ID that Needs to be Migrated This will be applied to all triggers

  

```perl

my $project_id_var = 1597;

```

>Changing the rollups logic since it requires the secret key pattern
>Search for “rollups” using “cmd + f”
>And change the secret key with respect to the project id 
>Eg : SELECT NEW.name ILIKE '%project_6icg2uge%' INTO secretkey_match;

```
change %project_<secretkey>%
```

2.2 Changing the “Bucardo.pm”  file

>Changing the project id in the copy/initial copy command
>Use “cmd + f” to replace the project_id=1597(this has to be previous project id) to desired project if needed
```
project_id=<desired_project_id>
```
>Changing the rollups table logic And change the secret key with respect to the project id 
```
change %%project_<secretkey>%%
```

## 3. Custom Bucardo Installation
3.0.1 Incase of Reinstallation of bucardo

>login to psql and drop database , user bucardo in bucardo machine and follow the rest of the commands

```SQL
psql
DROP DATABASE bucardo;
DROP USER bucardo;
\q
```

3.1 Clone the custom bucardo code
> remove the existing bucardo file(old code) in case it is present in your current directory(/home/postgres)
```bash
cd /home/postgres
rm -rf bucardo
```

```bash
git clone https://github.com/srinivasanmohankumar/bucardo.git --branch schema-migration-v3
```

3.2 Installation steps
```bash
cd bucardo
perl Makefile.PL
make
sudo make install
```
>bucardo configuration
```
bucardo install
```
>press 3 Enter 'postgres'
>press 4 Enter 'postgres'
>press 'P' Enter
>This should give database creation is successful message 

## 4.Bucardo Sync Setup

4.1 Adding Source Database
```bash
bucardo add database source host=<primary_db_ip> user=postgres db=ConvertFox_production dbpass=<password>
```
4.2 Adding Target Database
```bash
bucardo add database target host=<primary_db_ip> user=postgres db=ConvertFox_production dbpass=<password>
```

4.3 Adding Source Tables
```bash
bucardo add tables public.ahoy_messages public.article_feedbacks public.articles public.assignment_rules public.assignment_rule_triggers public.app_credentials public.automation_rules public.billings public.blacklisted_emails public.blocked_ips public.blocked_people public.bot_components public.bot_priorities public.bots public.bulk_imports public.campaigns public.cards_projects public.categories public.custom_email_templates public.chat_tags public.companies public.company_properties public.conversation_metrics public.conversation_properties public.conversation_replies_metrics public.conversations public.custom_team_mates public.deal_properties public.deals public.deleted_people public.ecommerce_carts public.ecommerce_categories public.ecommerce_checkouts public.ecommerce_customers public.ecommerce_orders public.ecommerce_products public.ecommerce_stores public.embedding_vectors public.emma_raw_sources public.emma_source_items public.event_categories public.events public.exports public.feature_tags public.form_data public.forms public.gist_webhooks public.imports public.inbound_email_addresses public.inbox_views public.integration_data public.kb_themes public.knowledgebase_migrations public.launch_urls public.live_people public.mail_filters public.mail_subscriptions public.meeting_links public.message_goals public.page_visit_urls public.people public.people_notes public.pipelines public.project_low_priorities public.project_roles public.project_subscription_histories public.property_categories public.satisfaction_ratings public.saved_replies public.segments public.setup_guides public.snippet_categories public.soft_bounced_emails public.spam_emails public.support_bot_analytics public.survey_themes public.surveys public.tags public.teams public.tours public.triggered_chats public.users_projects_roles public.webhook_subscriptions public.webhooks public.workflow_templates_users public.workflows public.activities public.article_page_visits public.articles_categories public.assignment_rule_actions public.automation_rule_actions public.automation_rule_people public.automation_rule_triggers public.bot_component_people public.bot_delay_component_details public.bot_sub_components public.campaigns_people public.chat_tags_messages public.choices public.company_notes public.conversation_message_tags public.conversation_property_options public.crm_emails public.crm_emails_people public.deal_and_company_activities public.deal_notes public.deal_notes_people public.deals_people public.email_accounts public.event_data_events public.failed_messages public.features_tags public.invoice_refund_histories public.mail_filters_links public.mail_filters_people public.mail_subscriptions_entities public.mail_subscriptions_people public.message_embeddings public.message_goals_people public.messages public.messages_users public.people_identifiers public.people_scheduled_meetings public.people_segments public.people_tags public.person_email_opens public.pg_search_documents public.product_categories public.questions public.responses public.rollups public.scheduled_meetings public.scheduled_meetings_users public.sessions public.stages public.support_bot_analytics_sources public.survey_responses public.teams_users public.tour_views public.triggered_chat_pending_people public.workflow_component_people public.workflow_components public.workflow_people relgroup=first_group db=source
```

4.4 Adding Target Database Tables
>Replace the secret key with appropriate project secret key
```bash
bucardo add tables <secret_key>.ahoy_messages <secret_key>.article_feedbacks <secret_key>.articles <secret_key>.assignment_rules <secret_key>.assignment_rule_triggers <secret_key>.app_credentials <secret_key>.automation_rules  <secret_key>.billings <secret_key>.blacklisted_emails <secret_key>.blocked_ips <secret_key>.blocked_people <secret_key>.bot_components <secret_key>.bot_priorities <secret_key>.bots <secret_key>.company_properties <secret_key>.bulk_imports <secret_key>.campaigns <secret_key>.cards_projects <secret_key>.categories <secret_key>.custom_email_templates <secret_key>.chat_tags <secret_key>.companies <secret_key>.conversation_metrics <secret_key>.conversation_properties <secret_key>.conversation_replies_metrics <secret_key>.conversations <secret_key>.custom_team_mates <secret_key>.deal_properties <secret_key>.deals <secret_key>.deleted_people <secret_key>.ecommerce_carts <secret_key>.ecommerce_categories <secret_key>.ecommerce_checkouts <secret_key>.ecommerce_customers <secret_key>.ecommerce_orders <secret_key>.ecommerce_products <secret_key>.ecommerce_stores <secret_key>.embedding_vectors <secret_key>.emma_raw_sources <secret_key>.emma_source_items <secret_key>.event_categories <secret_key>.events <secret_key>.exports <secret_key>.feature_tags <secret_key>.form_data <secret_key>.forms <secret_key>.gist_webhooks <secret_key>.imports <secret_key>.inbound_email_addresses <secret_key>.inbox_views <secret_key>.integration_data <secret_key>.kb_themes <secret_key>.knowledgebase_migrations <secret_key>.launch_urls <secret_key>.live_people <secret_key>.mail_filters <secret_key>.mail_subscriptions <secret_key>.meeting_links <secret_key>.message_goals <secret_key>.page_visit_urls <secret_key>.people <secret_key>.people_notes <secret_key>.pipelines <secret_key>.project_low_priorities <secret_key>.project_roles <secret_key>.project_subscription_histories <secret_key>.property_categories <secret_key>.satisfaction_ratings <secret_key>.saved_replies <secret_key>.segments <secret_key>.setup_guides <secret_key>.snippet_categories <secret_key>.soft_bounced_emails <secret_key>.spam_emails <secret_key>.support_bot_analytics <secret_key>.survey_themes <secret_key>.surveys <secret_key>.tags <secret_key>.teams <secret_key>.tours <secret_key>.triggered_chats <secret_key>.users_projects_roles <secret_key>.webhook_subscriptions <secret_key>.webhooks <secret_key>.workflow_templates_users <secret_key>.workflows <secret_key>.activities <secret_key>.article_page_visits <secret_key>.articles_categories <secret_key>.assignment_rule_actions <secret_key>.automation_rule_actions <secret_key>.automation_rule_people <secret_key>.automation_rule_triggers <secret_key>.bot_component_people <secret_key>.bot_delay_component_details <secret_key>.bot_sub_components <secret_key>.campaigns_people <secret_key>.chat_tags_messages <secret_key>.choices <secret_key>.company_notes <secret_key>.conversation_message_tags <secret_key>.conversation_property_options <secret_key>.crm_emails <secret_key>.crm_emails_people <secret_key>.deal_and_company_activities <secret_key>.deal_notes <secret_key>.deal_notes_people <secret_key>.deals_people <secret_key>.email_accounts <secret_key>.event_data_events <secret_key>.failed_messages <secret_key>.features_tags <secret_key>.invoice_refund_histories <secret_key>.mail_filters_links <secret_key>.mail_filters_people <secret_key>.mail_subscriptions_entities <secret_key>.mail_subscriptions_people <secret_key>.message_embeddings <secret_key>.message_goals_people <secret_key>.messages <secret_key>.messages_users <secret_key>.people_identifiers <secret_key>.people_scheduled_meetings <secret_key>.people_segments <secret_key>.people_tags <secret_key>.person_email_opens <secret_key>.pg_search_documents <secret_key>.product_categories <secret_key>.questions <secret_key>.responses <secret_key>.rollups <secret_key>.scheduled_meetings <secret_key>.scheduled_meetings_users <secret_key>.sessions <secret_key>.stages <secret_key>.support_bot_analytics_sources <secret_key>.survey_responses <secret_key>.teams_users <secret_key>.tour_views <secret_key>.triggered_chat_pending_people <secret_key>.workflow_component_people <secret_key>.workflow_components <secret_key>.workflow_people db=target
```

4.5 Adding custom names
>Change the appropriate project secret key
```bash
bucardo add customname public.ahoy_messages <secret_key>.ahoy_messages

bucardo add customname public.app_credentials <secret_key>.app_credentials

bucardo add customname public.article_feedbacks <secret_key>.article_feedbacks

bucardo add customname public.articles <secret_key>.articles

bucardo add customname public.assignment_rules <secret_key>.assignment_rules

bucardo add customname public.assignment_rule_triggers <secret_key>.assignment_rule_triggers

bucardo add customname public.automation_rules <secret_key>.automation_rules

bucardo add customname public.billings <secret_key>.billings

bucardo add customname public.blacklisted_emails <secret_key>.blacklisted_emails

bucardo add customname public.blocked_ips <secret_key>.blocked_ips

bucardo add customname public.blocked_people <secret_key>.blocked_people

bucardo add customname public.bot_components <secret_key>.bot_components

bucardo add customname public.bot_priorities <secret_key>.bot_priorities

bucardo add customname public.bots <secret_key>.bots

bucardo add customname public.company_properties <secret_key>.company_properties

bucardo add customname public.bulk_imports <secret_key>.bulk_imports

bucardo add customname public.campaigns <secret_key>.campaigns

bucardo add customname public.cards_projects <secret_key>.cards_projects

bucardo add customname public.categories <secret_key>.categories

bucardo add customname public.custom_email_templates <secret_key>.custom_email_templates

bucardo add customname public.chat_tags <secret_key>.chat_tags

bucardo add customname public.companies <secret_key>.companies

bucardo add customname public.conversation_metrics <secret_key>.conversation_metrics

bucardo add customname public.conversation_properties <secret_key>.conversation_properties

bucardo add customname public.conversation_replies_metrics <secret_key>.conversation_replies_metrics

bucardo add customname public.conversations <secret_key>.conversations

bucardo add customname public.custom_team_mates <secret_key>.custom_team_mates

bucardo add customname public.deal_properties <secret_key>.deal_properties

bucardo add customname public.deals <secret_key>.deals

bucardo add customname public.deleted_people <secret_key>.deleted_people

bucardo add customname public.ecommerce_carts <secret_key>.ecommerce_carts

bucardo add customname public.ecommerce_categories <secret_key>.ecommerce_categories

bucardo add customname public.ecommerce_checkouts <secret_key>.ecommerce_checkouts

bucardo add customname public.ecommerce_customers <secret_key>.ecommerce_customers

bucardo add customname public.ecommerce_orders <secret_key>.ecommerce_orders

bucardo add customname public.ecommerce_products <secret_key>.ecommerce_products

bucardo add customname public.ecommerce_stores <secret_key>.ecommerce_stores

bucardo add customname public.embedding_vectors <secret_key>.embedding_vectors

bucardo add customname public.emma_raw_sources <secret_key>.emma_raw_sources

bucardo add customname public.emma_source_items <secret_key>.emma_source_items

bucardo add customname public.event_categories <secret_key>.event_categories

bucardo add customname public.events <secret_key>.events

bucardo add customname public.exports <secret_key>.exports

bucardo add customname public.feature_tags <secret_key>.feature_tags

bucardo add customname public.form_data <secret_key>.form_data

bucardo add customname public.forms <secret_key>.forms

bucardo add customname public.gist_webhooks <secret_key>.gist_webhooks

bucardo add customname public.imports <secret_key>.imports

bucardo add customname public.inbound_email_addresses <secret_key>.inbound_email_addresses

bucardo add customname public.inbox_views <secret_key>.inbox_views

bucardo add customname public.integration_data <secret_key>.integration_data

bucardo add customname public.kb_themes <secret_key>.kb_themes

bucardo add customname public.knowledgebase_migrations <secret_key>.knowledgebase_migrations

bucardo add customname public.launch_urls <secret_key>.launch_urls

bucardo add customname public.live_people <secret_key>.live_people

bucardo add customname public.mail_filters <secret_key>.mail_filters

bucardo add customname public.mail_subscriptions <secret_key>.mail_subscriptions

bucardo add customname public.meeting_links <secret_key>.meeting_links

bucardo add customname public.message_goals <secret_key>.message_goals

bucardo add customname public.page_visit_urls <secret_key>.page_visit_urls

bucardo add customname public.people <secret_key>.people

bucardo add customname public.people_notes <secret_key>.people_notes

bucardo add customname public.pipelines <secret_key>.pipelines

bucardo add customname public.project_low_priorities <secret_key>.project_low_priorities

bucardo add customname public.project_roles <secret_key>.project_roles

bucardo add customname public.project_subscription_histories <secret_key>.project_subscription_histories

bucardo add customname public.property_categories <secret_key>.property_categories

bucardo add customname public.satisfaction_ratings <secret_key>.satisfaction_ratings

bucardo add customname public.saved_replies <secret_key>.saved_replies

bucardo add customname public.segments <secret_key>.segments

bucardo add customname public.setup_guides <secret_key>.setup_guides

bucardo add customname public.snippet_categories <secret_key>.snippet_categories

bucardo add customname public.soft_bounced_emails <secret_key>.soft_bounced_emails

bucardo add customname public.spam_emails <secret_key>.spam_emails

bucardo add customname public.support_bot_analytics <secret_key>.support_bot_analytics

bucardo add customname public.survey_themes <secret_key>.survey_themes

bucardo add customname public.surveys <secret_key>.surveys

bucardo add customname public.tags <secret_key>.tags

bucardo add customname public.teams <secret_key>.teams

bucardo add customname public.tours <secret_key>.tours

bucardo add customname public.triggered_chats <secret_key>.triggered_chats

bucardo add customname public.users_projects_roles <secret_key>.users_projects_roles

bucardo add customname public.webhook_subscriptions <secret_key>.webhook_subscriptions

bucardo add customname public.webhooks <secret_key>.webhooks

bucardo add customname public.workflow_templates_users <secret_key>.workflow_templates_users

bucardo add customname public.workflows <secret_key>.workflows

bucardo add customname public.activities <secret_key>.activities

bucardo add customname public.article_page_visits <secret_key>.article_page_visits

bucardo add customname public.articles_categories <secret_key>.articles_categories

bucardo add customname public.assignment_rule_actions <secret_key>.assignment_rule_actions

bucardo add customname public.automation_rule_actions <secret_key>.automation_rule_actions

bucardo add customname public.automation_rule_people <secret_key>.automation_rule_people

bucardo add customname public.automation_rule_triggers <secret_key>.automation_rule_triggers

bucardo add customname public.bot_component_people <secret_key>.bot_component_people

bucardo add customname public.bot_delay_component_details <secret_key>.bot_delay_component_details

bucardo add customname public.bot_sub_components <secret_key>.bot_sub_components

bucardo add customname public.campaigns_people <secret_key>.campaigns_people

bucardo add customname public.chat_tags_messages <secret_key>.chat_tags_messages

bucardo add customname public.choices <secret_key>.choices

bucardo add customname public.company_notes <secret_key>.company_notes

bucardo add customname public.conversation_message_tags <secret_key>.conversation_message_tags

bucardo add customname public.conversation_property_options <secret_key>.conversation_property_options

bucardo add customname public.crm_emails <secret_key>.crm_emails

bucardo add customname public.crm_emails_people <secret_key>.crm_emails_people

bucardo add customname public.deal_and_company_activities <secret_key>.deal_and_company_activities

bucardo add customname public.deal_notes <secret_key>.deal_notes

bucardo add customname public.deal_notes_people <secret_key>.deal_notes_people

bucardo add customname public.deals_people <secret_key>.deals_people

bucardo add customname public.email_accounts <secret_key>.email_accounts

bucardo add customname public.event_data_events <secret_key>.event_data_events

bucardo add customname public.failed_messages <secret_key>.failed_messages

bucardo add customname public.features_tags <secret_key>.features_tags

bucardo add customname public.invoice_refund_histories <secret_key>.invoice_refund_histories

bucardo add customname public.mail_filters_links <secret_key>.mail_filters_links

bucardo add customname public.mail_filters_people <secret_key>.mail_filters_people

bucardo add customname public.mail_subscriptions_entities <secret_key>.mail_subscriptions_entities

bucardo add customname public.mail_subscriptions_people <secret_key>.mail_subscriptions_people

bucardo add customname public.message_embeddings <secret_key>.message_embeddings

bucardo add customname public.message_goals_people <secret_key>.message_goals_people

bucardo add customname public.messages <secret_key>.messages

bucardo add customname public.messages_users <secret_key>.messages_users

bucardo add customname public.people_identifiers <secret_key>.people_identifiers

bucardo add customname public.people_scheduled_meetings <secret_key>.people_scheduled_meetings

bucardo add customname public.people_segments <secret_key>.people_segments

bucardo add customname public.people_tags <secret_key>.people_tags

bucardo add customname public.person_email_opens <secret_key>.person_email_opens

bucardo add customname public.pg_search_documents <secret_key>.pg_search_documents

bucardo add customname public.product_categories <secret_key>.product_categories

bucardo add customname public.questions <secret_key>.questions

bucardo add customname public.responses <secret_key>.responses

bucardo add customname public.rollups <secret_key>.rollups

bucardo add customname public.scheduled_meetings <secret_key>.scheduled_meetings

bucardo add customname public.scheduled_meetings_users <secret_key>.scheduled_meetings_users

bucardo add customname public.sessions <secret_key>.sessions

bucardo add customname public.stages <secret_key>.stages

bucardo add customname public.support_bot_analytics_sources <secret_key>.support_bot_analytics_sources

bucardo add customname public.survey_responses <secret_key>.survey_responses

bucardo add customname public.teams_users <secret_key>.teams_users

bucardo add customname public.tour_views <secret_key>.tour_views

bucardo add customname public.triggered_chat_pending_people <secret_key>.triggered_chat_pending_people

bucardo add customname public.workflow_component_people <secret_key>.workflow_component_people

bucardo add customname public.workflow_components <secret_key>.workflow_components

bucardo add customname public.workflow_people <secret_key>.workflow_people
```

4.6 Adding Sync
>Change the appropriate project secret key
```bash
bucardo add sync project_<secret_key>_sync relgroup=first_group dbs=source,target  onetimecopy=2
```

4.7 Restart Bucardo
```bash
sudo bucardo stop
sudo bucardo start
```

4.8 Check the status
```bash
bucardo status project_<secret_key>_sync
```
>If the status is good sync is completed and it is ready for migration 

4.9 Run Rake Task
>Ask Application Team to check the count to ensure the data is migrated for all tables

## 5.Tenant Switching

>Ask Application team to halt the service for that project

5.1 Stop the sync
```bash
sudo bucardo stop
```

5.2 Remove Triggers
>Log in to the production primary db(as postgres User) and execute the following
```SQL
DROP SCHEAMA bucardo CASCADE;
```
>Ensure the triggers are Removed

5.3 Set Sequence
> Set the sequence by running the following SQL script in the appropriate project schema search path
> 
>!!NOTE: change the appropriate secret key for both SQL commands
```SQL
\dn
SET search_path = '<secret_key>';
```

```SQL
DO $$ 
DECLARE
    rec RECORD;
    dat_type VARCHAR;
BEGIN
    -- Loop through each sequence that is owned by a table column
    FOR rec IN 
        SELECT 
            s.relname AS sequence_name,
	ns.nspname AS schema_name,
            t.relname AS table_name,
            a.attname AS column_name
        FROM 
            pg_class s
            JOIN pg_depend d ON d.objid = s.oid
            JOIN pg_class t ON d.refobjid = t.oid
            JOIN pg_attribute a ON a.attnum = d.refobjsubid AND a.attrelid = t.oid
	 JOIN pg_namespace ns ON ns.oid = t.relnamespace
        WHERE 
            s.relkind = 'S' AND d.classid = 'pg_class'::regclass AND
	 ns.nspname = '<secret_key>' 
    LOOP
        SELECT data_type
        INTO dat_type
        FROM information_schema.columns
        WHERE table_name = rec.table_name AND column_name = rec.column_name;

        IF(dat_type='integer' or dat_type='bigint') THEN

               -- Raise notice to see what is being processed
               RAISE NOTICE 'Adjusting sequence: % for table: % and column: %', rec.sequence_name, rec.table_name, rec.column_name;

               -- Perform setval for each sequence
               EXECUTE format(
                    'SELECT setval(''%I'', COALESCE((SELECT MAX(%I) FROM %I), 0) + 1, false)', 
                    rec.sequence_name, rec.column_name, rec.table_name
               );
        ELSE 
               RAISE NOTICE 'WARNING: Didnt Adjusting sequence: % for table: % and column: % due to %I', rec.sequence_name, rec.table_name, rec.column_name,dat_type;
        END IF;
    END LOOP;
END $$;
```

5.4 Enable Tenant 
>Ask the application team to enable the tenant which is migrated
 
 