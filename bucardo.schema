
-- Schema for the main Bucardo database
-- Version 5.6.0

-- Should be run as a superuser
-- This should not need to be run directly: use either
-- bucardo install
-- or
-- bucardo upgrade

\set ON_ERROR_STOP off
\echo NOTE: Creating bucardo prerequisites: user, database, plperl; can ignore any errors

-- Create the bucardo user and database if they don't already exist
SET client_min_messages = 'ERROR';

CREATE USER bucardo SUPERUSER;
CREATE DATABASE bucardo OWNER bucardo;

\c bucardo bucardo

-- Create the base bucardo schema and languages
SET client_min_messages = 'ERROR';
CREATE LANGUAGE plpgsql;
CREATE LANGUAGE plperlu;
CREATE SCHEMA bucardo;
ALTER DATABASE bucardo SET search_path = bucardo, public;
SET standard_conforming_strings = 'ON';

-- The above were allowed to fail, because there is no harm if the objects
-- already existed. From this point forward however, we suffer no errors

\echo NOTE: Done with prerequisite setup; errors no longer ignored

\set ON_ERROR_STOP on

BEGIN;
SET client_min_messages = 'WARNING';
SET search_path TO bucardo;
SET escape_string_warning = 'OFF';


-- Try and create a plperlu function, then call it at the very end
-- Do not change this string, as the bucardo program parses it
CREATE OR REPLACE FUNCTION bucardo.plperlu_test()
RETURNS TEXT
LANGUAGE plperlu
AS $bc$
return 'Pl/PerlU was successfully installed';
$bc$;

--
-- Main bucardo configuration information
--
CREATE TABLE bucardo.bucardo_config (
  name     TEXT        NOT NULL, -- short unique name, maps to %config inside Bucardo
  setting  TEXT        NOT NULL,
  defval   TEXT            NULL, -- the default value for this setting, per initial config
  about    TEXT            NULL, -- long description
  type     TEXT            NULL, -- sync or goat
  item     TEXT            NULL, -- which specific sync or goat
  cdate    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.bucardo_config IS $$Contains configuration variables for a specific Bucardo instance$$;

CREATE UNIQUE INDEX bucardo_config_unique ON bucardo.bucardo_config(LOWER(name)) WHERE item IS NULL;

CREATE UNIQUE INDEX bucardo_config_unique_name ON bucardo.bucardo_config(name,item,type) WHERE item IS NOT NULL;

ALTER TABLE bucardo.bucardo_config ADD CONSTRAINT valid_config_type CHECK (type IN ('sync','goat'));

ALTER TABLE bucardo.bucardo_config ADD CONSTRAINT valid_config_isolation_level
  CHECK (name <> 'isolation_level' OR (setting IN ('serializable','repeatable read')));

CREATE FUNCTION bucardo.check_bucardo_config()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $bc$
BEGIN
  IF NEW.name <> ALL('{log_conflict_file,warning_file,email_debug_file,flatfile_dir,reason_file,stats_script_url,stopfile,log_timer_format}') THEN
    NEW.setting = LOWER(NEW.setting);
  END IF;

  IF (NEW.type IS NOT NULL and NEW.item IS NULL) THEN
    RAISE EXCEPTION 'Must provide a specific %', NEW.type;
  END IF;

  IF (NEW.item IS NOT NULL and NEW.type IS NULL) THEN
    RAISE EXCEPTION 'Must provide a type if giving a name';
  END IF;

  IF (NEW.name = 'sync' OR NEW.name = 'goat') THEN
    RAISE EXCEPTION 'Invalid configuration name';
  END IF;

  RETURN NEW;

END;
$bc$;

COMMENT ON FUNCTION bucardo.check_bucardo_config() IS $$Basic sanity checks for configuration items$$;

CREATE TRIGGER check_bucardo_config
  BEFORE INSERT OR UPDATE ON bucardo.bucardo_config
  FOR EACH ROW EXECUTE PROCEDURE bucardo.check_bucardo_config();

-- Sleep times (all in seconds)
COPY bucardo.bucardo_config("name",setting,about)
FROM STDIN
WITH DELIMITER '|';
mcp_loop_sleep|0.2|How long does the main MCP daemon sleep between loops?
mcp_dbproblem_sleep|15|How many seconds to sleep before trying to respawn
mcp_vactime|60|How often in seconds do we check that a VAC is still running?
ctl_sleep|0.2|How long does the controller loop sleep?
kid_sleep|0.5|How long does a kid loop sleep?
kid_nodeltarows_sleep|0.5|How long do kids sleep if no delta rows are found?
kid_serial_sleep|0.5|How long to sleep in seconds if we hit a serialization error
kid_deadlock_sleep|0.5|How long to sleep in seconds if we hit a deadlock error
kid_restart_sleep|1|How long to sleep in seconds when restarting a kid?
endsync_sleep|1.0|How long do we sleep when custom code requests an endsync?
vac_sleep|120|How long does VAC process sleep between runs?
vac_run|30|How often does the VAC process run?
\.


-- Various timeouts (times are in seconds)
COPY bucardo.bucardo_config("name",setting,about)
FROM STDIN
WITH DELIMITER '|';
mcp_pingtime|60|How often do we ping check the MCP?
kid_pingtime|60|How often do we ping check the KID?
ctl_checkonkids_time|10|How often does the controller check on the kids health?
ctl_createkid_time|0.5|How long do we sleep to allow kids-on-demand to get on their feet?
tcp_keepalives_idle|0|How long to wait between each keepalive probe.
tcp_keepalives_interval|0|How long to wait for a response to a keepalive probe.
tcp_keepalives_count|0|How many probes to send. 0 indicates sticking with system defaults.
reload_config_timeout|30|How long to wait for reload_config to finish.
\.


-- Logging
COPY bucardo.bucardo_config(name,setting,about)
FROM STDIN
WITH DELIMITER '|';
log_microsecond|0|Show microsecond output in the timestamps?
log_showpid|1|Show PID in the log output?
log_showlevel|0|Show log level in the log output?
log_showline|0|Show line number in the log output?
log_showtime|3|Show timestamp in the log output?  0=off  1=seconds since epoch  2=scalar gmtime  3=scalar localtime
log_timer_format||Show timestamps in specific format; default/empty to show time from scalar
log_conflict_file|bucardo_conflict.log|Name of the conflict detail log file
log_showsyncname|1|Show the name of the sync next to the 'KID' prefix
log_level|NORMAL|How verbose to make the logging. Higher is more verbose.
warning_file|bucardo.warning.log|File containing all log lines starting with "Warning"
\.

-- Versioning
COPY bucardo.bucardo_config(name,setting,about)
FROM STDIN
WITH DELIMITER '|';
bucardo_initial_version|5.6.0|Bucardo version this schema was created with
bucardo_version|5.6.0|Current version of Bucardo
\.

-- Other settings:
COPY bucardo.bucardo_config(name,setting,about)
FROM STDIN
WITH DELIMITER '|';
bucardo_vac|1|Do we want the automatic VAC daemon to run?
default_email_from|nobody@example.com|Who the alert emails are sent as
default_email_to|nobody@example.com|Who to send alert emails to
default_email_host|localhost|Which host to send email through
default_email_port|25|Which port to send email through
default_conflict_strategy|bucardo_latest|Default conflict strategy for all syncs
email_debug_file||File to save a copy of all outgoing emails to
email_auth_user||User to use for email authentication via Net::SMTP
email_auth_pass||Password to use for email authentication via Net::SMTP
flatfile_dir|.|Directory to store the flatfile output inside of
host_safety_check||Regex to make sure we don't accidentally run where we should not
isolation_level|repeatable read|Default isolation level: can be serializable or repeatable read
piddir|/var/run/bucardo|Directory holding Bucardo PID files
quick_delta_check|1|Whether to do a quick scan of delta activity
reason_file|bucardo.restart.reason.txt|File to hold reasons for stopping and starting
semaphore_table|bucardo_status|Table to let apps know a sync is ongoing
statement_chunk_size|6000|How many primary keys to shove into a single statement
stats_script_url|http://www.bucardo.org/|Location of the stats script
stopfile|fullstopbucardo|Name of the semaphore file used to stop Bucardo processes
syslog_facility|LOG_LOCAL1|Which syslog facility level to use
\.

-- Unused at the moment:
COPY bucardo.bucardo_config(name,setting,about)
FROM STDIN
WITH DELIMITER '|';
autosync_ddl|newcol|Which DDL changing conditions do we try to remedy automatically?
\.

-- This needs to run after all population of bucardo.config
UPDATE bucardo.bucardo_config SET defval = setting;

--
-- Keep track of every database we need to connect to
--
CREATE TABLE bucardo.db (
  name                 TEXT        NOT NULL,  -- local name for convenience, not necessarily database name
                         CONSTRAINT db_name_pk PRIMARY KEY (name),
  dbdsn                TEXT        NOT NULL DEFAULT '',
  dbtype               TEXT        NOT NULL DEFAULT 'postgres',
  dbhost               TEXT            NULL DEFAULT '',
  dbport               TEXT            NULL DEFAULT '',
  dbname               TEXT            NULL, -- the actual name of the database, not the primary key 'local' name
  dbuser               TEXT            NULL,
  dbpass               TEXT            NULL,
  dbconn               TEXT        NOT NULL DEFAULT '',  -- string to add to the generated dsn
  dbservice            TEXT            NULL DEFAULT '',
  pgpass               TEXT            NULL,      -- local file with connection info same as pgpass
  status               TEXT        NOT NULL DEFAULT 'active',
  server_side_prepares BOOLEAN     NOT NULL DEFAULT true,
  makedelta            BOOLEAN     NOT NULL DEFAULT false,
  cdate                TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.db IS $$Holds information about each database used in replication$$;

ALTER TABLE bucardo.db ADD CONSTRAINT db_status CHECK (status IN ('active','inactive','stalled'));

ALTER TABLE bucardo.db ADD CONSTRAINT db_service_valid CHECK (dbservice IS NOT NULL OR dbname IS NOT NULL AND dbuser IS NOT NULL AND dbhost IS NOT NULL AND dbport IS NOT NULL);

--
-- Databases can belong to zero or more named groups
--
CREATE TABLE bucardo.dbgroup (
  name      TEXT        NOT NULL,
              CONSTRAINT dbgroup_name_pk PRIMARY KEY (name),
  about     TEXT            NULL,
  cdate     TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.dbgroup IS $$Named groups of databases: used as 'targetgroup' for syncs$$;

CREATE TABLE bucardo.dbmap (
  db        TEXT        NOT NULL,
              CONSTRAINT  dbmap_db_fk FOREIGN KEY (db) REFERENCES bucardo.db(name) ON UPDATE CASCADE ON DELETE CASCADE,
  dbgroup   TEXT        NOT NULL,
              CONSTRAINT  dbmap_dbgroup_fk FOREIGN KEY (dbgroup) REFERENCES bucardo.dbgroup(name) ON UPDATE CASCADE ON DELETE CASCADE,
  priority  SMALLINT    NOT NULL DEFAULT 0,
  role      TEXT        NOT NULL DEFAULT 'target',
  cdate     TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.dbmap IS $$Associates a database with one or more groups$$;

CREATE UNIQUE INDEX dbmap_unique ON bucardo.dbmap(db,dbgroup);


--
-- Track status information about each database
--
CREATE TABLE bucardo.db_connlog (
  db          TEXT        NOT NULL,
                CONSTRAINT  db_connlog_dbid_fk FOREIGN KEY (db) REFERENCES bucardo.db(name) ON UPDATE CASCADE ON DELETE CASCADE,
  conndate    TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when we first connected to it
  connstring  TEXT        NOT NULL,
  status      TEXT        NOT NULL DEFAULT 'unknown',
                CONSTRAINT db_connlog_status CHECK (status IN ('unknown', 'good', 'down', 'unreachable')),
  version     TEXT            NULL
);
COMMENT ON TABLE bucardo.db_connlog IS $$Tracks connection attempts to each database when its information changes$$;

--
-- We need to track each item we want to replicate from or replicate to
--
CREATE SEQUENCE bucardo.goat_id_seq;
CREATE TABLE bucardo.goat (
  id                 INTEGER     NOT NULL DEFAULT nextval('goat_id_seq'),
                       CONSTRAINT goat_id_pk PRIMARY KEY (id),
  db                 TEXT        NOT NULL,
                       CONSTRAINT goat_db_fk FOREIGN KEY (db) REFERENCES bucardo.db(name) ON UPDATE CASCADE ON DELETE RESTRICT,
  schemaname         TEXT        NOT NULL,
  tablename          TEXT        NOT NULL,
  reltype            TEXT        NOT NULL DEFAULT 'table',
  pkey               TEXT            NULL,
  qpkey              TEXT            NULL,
  pkeytype           TEXT            NULL,
  has_delta          BOOLEAN     NOT NULL DEFAULT false,
  autokick           BOOLEAN         NULL,                 -- overrides sync-level autokick
  conflict_strategy  TEXT            NULL,
  makedelta          TEXT            NULL,
  rebuild_index      SMALLINT        NULL,               -- overrides sync-level rebuild_index
  ghost              BOOLEAN     NOT NULL DEFAULT false, -- only drop triggers, do not replicate
  analyze_after_copy BOOLEAN     NOT NULL DEFAULT true,
  vacuum_after_copy  BOOLEAN     NOT NULL DEFAULT true,
  strict_checking    BOOLEAN     NOT NULL DEFAULT true,
  delta_bypass       BOOLEAN     NOT NULL DEFAULT false,
  delta_bypass_min   BIGINT          NULL,
  delta_bypass_count BIGINT          NULL,
  delta_bypass_percent   SMALLINT    NULL,
  cdate              TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.goat IS $$Holds information on each table or sequence that may be replicated$$;

ALTER TABLE bucardo.goat ADD CONSTRAINT has_schemaname CHECK (length(schemaname) >= 1);

ALTER TABLE bucardo.goat ADD CONSTRAINT valid_reltype CHECK (reltype IN ('table','sequence'));

ALTER TABLE bucardo.goat ADD CONSTRAINT pkey_needs_type CHECK (pkey = '' OR pkeytype IS NOT NULL);


--
-- Set of filters for each goat.
--
CREATE SEQUENCE bucardo.bucardo_custom_trigger_id_seq;
CREATE TABLE bucardo.bucardo_custom_trigger (
  id                INTEGER     NOT NULL DEFAULT nextval('bucardo_custom_trigger_id_seq'),
      CONSTRAINT bucardo_custom_trigger_id_pk PRIMARY KEY (id),
  goat              INTEGER     NOT NULL,
      CONSTRAINT bucardo_custom_trigger_goat_fk FOREIGN KEY (goat) REFERENCES bucardo.goat(id) ON DELETE CASCADE,
  trigger_name      TEXT        NOT NULL,
  trigger_type      TEXT        NOT NULL,
  trigger_language  TEXT        NOT NULL DEFAULT 'plpgsql',
  trigger_body      TEXT        NOT NULL,
  trigger_level     TEXT        NOT NULL,
  status            TEXT        NOT NULL DEFAULT 'active',
  cdate             TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.bucardo_custom_trigger IS $$Used to override the default bucardo_delta trigger on a per-table basis$$;

ALTER TABLE bucardo.bucardo_custom_trigger ADD CONSTRAINT type_is_delta_or_trigger CHECK (trigger_type IN ('delta', 'triggerkick'));

ALTER TABLE bucardo.bucardo_custom_trigger ADD CONSTRAINT level_is_row_statement CHECK (trigger_level IN ('ROW', 'STATEMENT'));

CREATE UNIQUE INDEX bucardo_custom_trigger_goat_type_unique ON bucardo.bucardo_custom_trigger(goat, trigger_type);

--
-- A group of goats. Ideally arranged in some sort of tree.
--
CREATE TABLE bucardo.herd (
  name       TEXT        NOT NULL,
               CONSTRAINT herd_name_pk PRIMARY KEY (name),
  about      TEXT            NULL,
  cdate      TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.herd IS $$Named group of tables or sequences from the goat table: used as the 'source' for syncs$$;

--
-- Goats belong to zero or more herds. In most cases, they will 
-- belong to a single herd if they are being replicated.
--
CREATE TABLE bucardo.herdmap (
  herd      TEXT        NOT NULL,
              CONSTRAINT herdmap_herd_fk FOREIGN KEY (herd) REFERENCES bucardo.herd(name) ON UPDATE CASCADE ON DELETE CASCADE,
  goat      INTEGER     NOT NULL,
              CONSTRAINT herdmap_goat_fk FOREIGN KEY (goat) REFERENCES bucardo.goat(id) ON DELETE CASCADE,
  priority  SMALLINT    NOT NULL DEFAULT 0,
  cdate     TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.herdmap IS $$Associates a goat with one or more herds$$;

CREATE UNIQUE INDEX bucardo_herdmap_unique ON bucardo.herdmap(herd,goat);

CREATE FUNCTION bucardo.herdcheck()
RETURNS TRIGGER
LANGUAGE plpgsql
AS
$bc$
BEGIN

-- All goats in a herd must be from the same database
PERFORM herd FROM herdmap h, goat g WHERE h.goat=g.id GROUP BY 1 HAVING COUNT(DISTINCT db) > 1;

IF FOUND THEN
  RAISE EXCEPTION 'All tables must within a relgroup must be from the same database';
END IF;

RETURN NEW;

END;
$bc$;

CREATE TRIGGER herdcheck
  AFTER INSERT OR UPDATE ON bucardo.herdmap
  FOR EACH ROW EXECUTE PROCEDURE bucardo.herdcheck();


--
-- We need to know who is replicating to who, and how
--
CREATE TABLE bucardo.sync (
  name               TEXT        NOT NULL UNIQUE,
                       CONSTRAINT sync_name_pk PRIMARY KEY (name),
  herd               TEXT            NULL,
                       CONSTRAINT sync_herd_fk FOREIGN KEY (herd) REFERENCES bucardo.herd(name) ON UPDATE CASCADE ON DELETE RESTRICT,
  dbs                TEXT            NULL,
                       CONSTRAINT sync_dbs_fk FOREIGN KEY (dbs) REFERENCES bucardo.dbgroup(name) ON UPDATE CASCADE ON DELETE RESTRICT,
  stayalive          BOOLEAN     NOT NULL DEFAULT true, -- Does the sync controller stay connected?
  kidsalive          BOOLEAN     NOT NULL DEFAULT true, -- Do the children stay connected?
  conflict_strategy  TEXT        NOT NULL DEFAULT 'bucardo_latest',
  copyextra          TEXT        NOT NULL DEFAULT '',  -- e.g. WITH OIDS
  deletemethod       TEXT        NOT NULL DEFAULT 'delete',
  autokick           BOOLEAN     NOT NULL DEFAULT true,      -- Are we issuing NOTICES via triggers?
  checktime          INTERVAL        NULL,                   -- How often to check if we've not heard anything?
  status             TEXT        NOT NULL DEFAULT 'active',  -- Possibly CHECK / FK ('stopped','paused','b0rken')
  rebuild_index      SMALLINT    NOT NULL DEFAULT 0,     -- Load without indexes and then REINDEX table
  priority           SMALLINT    NOT NULL DEFAULT 0,     -- Higher is better
  analyze_after_copy BOOLEAN     NOT NULL DEFAULT true,
  vacuum_after_copy  BOOLEAN     NOT NULL DEFAULT false,
  strict_checking    BOOLEAN     NOT NULL DEFAULT true,
  overdue            INTERVAL    NOT NULL DEFAULT '0 seconds'::interval,
  expired            INTERVAL    NOT NULL DEFAULT '0 seconds'::interval,
  track_rates        BOOLEAN     NOT NULL DEFAULT false,
  onetimecopy        SMALLINT    NOT NULL DEFAULT 0,
  lifetime           INTERVAL        NULL,                   -- force controller and kids to restart
  maxkicks           INTEGER     NOT NULL DEFAULT 0,         -- force controller and kids to restart
  isolation_level    TEXT            NULL,
  cdate              TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.sync IS $$Defines a single replication event from a herd to one or more target databases$$;

ALTER TABLE bucardo.sync ADD CONSTRAINT sync_deletemethod CHECK (deletemethod IN ('truncate', 'delete', 'truncate_cascade'));

-- Because NOTIFY is broke, make sure our names are simple:
ALTER TABLE bucardo.db      ADD CONSTRAINT db_name_sane      CHECK (name ~ E'^[a-zA-Z]\\w*$');

ALTER TABLE bucardo.dbgroup ADD CONSTRAINT dbgroup_name_sane CHECK (name ~ E'^[a-zA-Z]\\w*$');

ALTER TABLE bucardo.sync    ADD CONSTRAINT sync_name_sane
  CHECK (name ~ E'^[a-zA-Z]\\w*$' AND (lower(name) NOT IN ('pushdelta','fullcopy','swap','sync')));

ALTER TABLE bucardo.sync    ADD CONSTRAINT sync_isolation_level
  CHECK (isolation_level IS NULL OR (lower(isolation_level) IN ('serializable', 'repeatable read')));

CREATE SEQUENCE bucardo.clone_id_seq;
CREATE TABLE bucardo.clone (
  id        INTEGER     NOT NULL DEFAULT nextval('clone_id_seq'),
              CONSTRAINT clone_id_pk PRIMARY KEY (id),
  sync      TEXT            NULL,
    CONSTRAINT clone_sync_fk FOREIGN KEY (sync) REFERENCES bucardo.sync(name) ON UPDATE CASCADE ON DELETE CASCADE,
  dbgroup   TEXT            NULL,
    CONSTRAINT clone_dbgroup_fk FOREIGN KEY (dbgroup) REFERENCES bucardo.dbgroup(name) ON UPDATE CASCADE ON DELETE CASCADE,
  relgroup  TEXT            NULL,
    CONSTRAINT clone_relgroup_fk FOREIGN KEY (relgroup) REFERENCES bucardo.herd(name) ON UPDATE CASCADE ON DELETE CASCADE,
  options   TEXT            NULL,
  status    TEXT            NULL,
  started   TIMESTAMPTZ     NULL,
  ended     TIMESTAMPTZ     NULL,
  summary   TEXT            NULL,
  cdate     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE SEQUENCE bucardo.customcode_id_seq;
CREATE TABLE bucardo.customcode (
  id        INTEGER     NOT NULL DEFAULT nextval('customcode_id_seq'),
              CONSTRAINT customcode_id_pk PRIMARY KEY (id),
  name      TEXT        NOT NULL UNIQUE,
  about     TEXT            NULL,
  whenrun   TEXT        NOT NULL,
  getdbh    BOOLEAN     NOT NULL DEFAULT true,
  src_code  TEXT        NOT NULL,
  status    TEXT        NOT NULL DEFAULT 'active',
  priority  SMALLINT    NOT NULL DEFAULT 0,    
  cdate     TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.customcode IS $$Holds Perl subroutines that run via hooks in the replication process$$;

ALTER TABLE bucardo.customcode ADD CONSTRAINT customcode_whenrun
  CHECK (whenrun IN ('before_txn',
  'before_check_rows',
  'before_trigger_disable',
  'after_trigger_disable',
  'after_table_sync',
  'exception',
  'conflict',
  'before_trigger_enable',
  'after_trigger_enable',
  'after_txn',
  'before_sync',
  'after_sync'));

CREATE TABLE bucardo.customcode_map (
  code     INTEGER     NOT NULL,
             CONSTRAINT customcode_map_code_fk FOREIGN KEY (code) REFERENCES bucardo.customcode(id) ON DELETE CASCADE,
  sync     TEXT            NULL,
             CONSTRAINT customcode_map_sync_fk FOREIGN KEY (sync) REFERENCES bucardo.sync(name) ON UPDATE CASCADE ON DELETE SET NULL,
  goat     INTEGER         NULL,
             CONSTRAINT customcode_map_goat_fk FOREIGN KEY (goat) REFERENCES bucardo.goat(id) ON DELETE SET NULL,
  active   BOOLEAN     NOT NULL DEFAULT true,
  priority SMALLINT    NOT NULL DEFAULT 0,
  cdate    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.customcode_map IS $$Associates a custom code with one or more syncs or goats$$;

ALTER TABLE bucardo.customcode_map ADD CONSTRAINT customcode_map_syncgoat
  CHECK (sync IS NULL OR goat IS NULL);

CREATE UNIQUE INDEX customcode_map_unique_sync ON bucardo.customcode_map(code,sync) WHERE sync IS NOT NULL;
CREATE UNIQUE INDEX customcode_map_unique_goat ON bucardo.customcode_map(code,goat) WHERE goat IS NOT NULL;

--
-- Allow the target's names to differ from the source
--
CREATE SEQUENCE bucardo.customname_id_seq;
CREATE TABLE bucardo.customname (
  id       INTEGER      NOT NULL DEFAULT nextval('customname_id_seq'),
                          CONSTRAINT customname_id_pk PRIMARY KEY (id),
  goat     INTEGER      NOT NULL,
  newname  TEXT             NULL,
  db       TEXT             NULL,
  sync     TEXT             NULL,
  cdate    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE bucardo.customname ADD CONSTRAINT customname_sane_name
  CHECK (newname ~ E'^["a-zA-Z 0-9_.~]+$');

ALTER TABLE bucardo.customname
  ADD CONSTRAINT customname_db_fk
  FOREIGN KEY (db) REFERENCES bucardo.db (name)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE bucardo.customname
  ADD CONSTRAINT customname_sync_fk
  FOREIGN KEY (sync) REFERENCES bucardo.sync (name)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE bucardo.customname
  ADD CONSTRAINT customname_goat_fk
  FOREIGN KEY (goat) REFERENCES bucardo.goat (id)
  ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Allow the target's columns to differ from the source
--
CREATE SEQUENCE bucardo.customcols_id_seq;
CREATE TABLE bucardo.customcols (
  id       INTEGER      NOT NULL DEFAULT nextval('customcols_id_seq'),
                          CONSTRAINT customcols_id_pk PRIMARY KEY (id),
  goat     INTEGER      NOT NULL,
  clause   TEXT             NULL,
  db       TEXT             NULL,
  sync     TEXT             NULL,
  cdate    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE bucardo.customcols
  ADD CONSTRAINT customcols_db_fk
  FOREIGN KEY (db) REFERENCES bucardo.db (name)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE bucardo.customcols
  ADD CONSTRAINT customcols_sync_fk
  FOREIGN KEY (sync) REFERENCES bucardo.sync (name)
  ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE bucardo.customcols
  ADD CONSTRAINT customcols_goat_fk
  FOREIGN KEY (goat) REFERENCES bucardo.goat (id)
  ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Keep track of syncs as they run: provides instant and historical status information
--
CREATE TABLE bucardo.syncrun (
  sync      TEXT             NULL,
  truncates INTEGER      NOT NULL DEFAULT 0,
  deletes   BIGINT       NOT NULL DEFAULT 0,
  inserts   BIGINT       NOT NULL DEFAULT 0,
  conflicts BIGINT       NOT NULL DEFAULT 0,
  started   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  ended     TIMESTAMPTZ      NULL,
  lastgood  BOOLEAN      NOT NULL DEFAULT false,
  lastbad   BOOLEAN      NOT NULL DEFAULT false,
  lastempty BOOLEAN      NOT NULL DEFAULT false,
  details   TEXT             NULL,
  status    TEXT             NULL
);
COMMENT ON TABLE bucardo.syncrun IS $$Information about specific runs of syncs$$;

-- Link back to the sync table, but never lose the data even on a sync drop
ALTER TABLE bucardo.syncrun
  ADD CONSTRAINT syncrun_sync_fk
  FOREIGN KEY (sync) REFERENCES bucardo.sync (name)
  ON UPDATE CASCADE ON DELETE SET NULL;

-- Is essentially a unique index, but we want to avoid any [b]locking
CREATE INDEX syncrun_sync_started ON syncrun(sync) WHERE ended IS NULL;

-- We often need the last good/bad/empty for a sync:

CREATE INDEX syncrun_sync_lastgood ON syncrun(sync) WHERE lastgood IS TRUE;

CREATE INDEX syncrun_sync_lastbad ON syncrun(sync) WHERE lastbad IS TRUE;

CREATE INDEX syncrun_sync_lastempty ON syncrun(sync) WHERE lastempty IS TRUE;

--
-- Keep track of which dbs are currently being used, for traffic control
--
CREATE TABLE bucardo.dbrun (
  sync       TEXT         NOT NULL,
  dbname     TEXT         NOT NULL,
  pgpid      INTEGER      NOT NULL,
  started    TIMESTAMPTZ  NOT NULL  DEFAULT now()
);
COMMENT ON TABLE bucardo.dbrun IS $$Information about which databases are being accessed$$;

CREATE INDEX dbrun_index ON bucardo.dbrun(sync);


CREATE FUNCTION bucardo.table_exists(text,text)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $bc$
BEGIN
  PERFORM 1
    FROM pg_catalog.pg_class c, pg_namespace n
    WHERE c.relnamespace = n.oid
    AND n.nspname = $1
    AND c.relname = $2;
  IF FOUND THEN RETURN true; END IF;
  RETURN false;
END;
$bc$;


--
-- Return a safe/standard name for a table, for use in delta/track namings
--
CREATE OR REPLACE FUNCTION bucardo.bucardo_tablename_maker(text)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $bc$
DECLARE
  tname TEXT;
  newname TEXT;
  hashed TEXT;
BEGIN
  -- sanitize and dequote the table name to avoid double-quoting later
  SELECT INTO tname REGEXP_REPLACE(
    REPLACE($1, '.', '_'), -- replace dots
    '"(")?',
    '\1',
    'g'
  );

  -- Assumes max_identifier_length is 63
  -- Because even if not, we'll still abbreviate for consistency and portability
  SELECT INTO newname SUBSTRING(tname FROM 1 FOR 57);
  IF (newname != tname) THEN
    SELECT INTO newname SUBSTRING(tname FROM 1 FOR 46)
      || '!'
      || SUBSTRING(MD5(tname) FROM 1 FOR 10);
  END IF;

  -- We let Postgres worry about the quoting details
  SELECT INTO newname quote_ident(newname);

  RETURN newname;
END;
$bc$;


--
-- Return a created connection string from the db table
--
CREATE OR REPLACE FUNCTION bucardo.db_getconn(text)
RETURNS TEXT
LANGUAGE plperlu
SECURITY DEFINER
AS $bc$

## Given the name of a db, return the type, plus type-specific connection information
## ALL: the string 'DSN', a colon, and the value of the dbdsn field, if set
## Postgres: a connection string, username, password, and attribs
## Drizzle: a connection string, username, and password
## Firebird: a connection string, username, and password
## Mongo: "foo: bar" style connection information, one per line
## MariaDB: a connection string, username, and password
## MySQL: a connection string, username, and password
## Oracle: a connection string, username, and password
## Redis: "foo: bar" style connection information, one per line
## SQLite: a database file name

use strict;
use warnings;
use DBI;
my ($name, $SQL, $rv, $row, %db);

$name = shift;

$name =~ s/'/''/go;
$SQL = "SELECT * FROM bucardo.db WHERE name = '$name'";
$rv = spi_exec_query($SQL);
if (!$rv->{processed}) {
    elog(ERROR, qq{Error: Could not find a database with a name of $name\n});
}
$row = $rv->{rows}[0];

my $dbtype = $row->{dbtype} || 'postgres';

## If we have a DSN, it trumps everything else
if (exists $row->{dbdsn} and length $row->{dbdsn}) {
    return "$dbtype\nDSN:$row->{dbdsn}\n\n\n";
}


for (qw(host port name user pass conn service)) {
    $db{$_} = exists $row->{"db$_"} ? $row->{"db$_"} : '';
}

## Check that the port is numeric
if (defined $db{port} and length $db{port} and $db{port} !~ /^\d+$/) {
    elog(ERROR, qq{Database port must be numeric, but got "$db{port}"\n});
}

if ($dbtype eq 'postgres') {
    ## If there is a dbfile and it exists, it overrides the rest
    ## Format = hostname:port:database:username:password
    ## http://www.postgresql.org/docs/current/static/libpq-pgpass.html

    ## We also check for one if no password is given
    if (!defined $row->{dbpass}) {
        my $passfile = $row->{pgpass} || '';
        if (open my $pass, "<", $passfile) {
            ## We only do complete matches
            my $match = "$row->{dbhost}:$row->{dbport}:$row->{dbname}:$row->{dbuser}";
            while (<$pass>) {
                if (/^$match:(.+)/) {
                    $row->{dbpass} = $1;
                    elog(DEBUG, "Found password in pgpass file $passfile for $match");
                    last;
                }
            }
        }
    }

    ## These may be specified in the service name
    $db{service} = '' if ! defined $db{service};
    if (! length($db{service})) {
        length $db{name} or elog(ERROR, qq{Database name is mandatory\n});
        length $db{user} or elog(ERROR, qq{Database username is mandatory\n});
    }

    my $connstring = "dbi:Pg:";
    $db{host} ||= ''; $db{port} ||= ''; $db{pass} ||= ''; $db{user} ||= '';
    $connstring .= join ';', map {
        ( $_ eq 'name' ? 'dbname' : $_ ) . "=$db{$_}";
    } grep { length $db{$_} } qw/name host port service/;

    $connstring .= ';' . $db{conn} if length $db{conn};

    my $ssp = $row->{server_side_prepares};
    $ssp = 1 if ! defined $ssp;

    return "$dbtype\n$connstring\n$db{user}\n$db{pass}\n$ssp";

} ## end postgres

if ($dbtype eq 'drizzle') {

    length $db{name} or elog(ERROR, qq{Database name is mandatory\n});
    length $db{user} or elog(ERROR, qq{Database username is mandatory\n});

    my $connstring = "dbi:drizzle:database=$db{name}";
    $db{host} ||= ''; $db{port} ||= ''; $db{pass} ||= '';
    length $db{host} and $connstring .= ";host=$db{host}";
    length $db{port} and $connstring .= ";port=$db{port}";
    length $db{conn} and $connstring .= ";$db{conn}";

    return "$dbtype\n$connstring\n$db{user}\n$db{pass}";

} ## end drizzle

if ($dbtype eq 'mongo') {
   my $connstring = "$dbtype\n";
   for my $name (qw/ host port user pass /) {
       defined $db{$name} and length $db{$name} and $connstring .= "$name: $db{$name}\n";
   }
   chomp $connstring;
   return $connstring;
}

if ($dbtype eq 'mysql' or $dbtype eq 'mariadb') {

    length $db{name} or elog(ERROR, qq{Database name is mandatory\n});
    length $db{user} or elog(ERROR, qq{Database username is mandatory\n});

    my $connstring = "dbi:mysql:database=$db{name}";
    $db{host} ||= ''; $db{port} ||= ''; $db{pass} ||= '';
    length $db{host} and $connstring .= ";host=$db{host}";
    length $db{port} and $connstring .= ";port=$db{port}";
    length $db{conn} and $connstring .= ";$db{conn}";

    return "$dbtype\n$connstring\n$db{user}\n$db{pass}";

} ## end mysql/mariadb

if ($dbtype eq 'firebird') {

    length $db{name} or elog(ERROR, qq{Database name is mandatory\n});
    length $db{user} or elog(ERROR, qq{Database username is mandatory\n});

    my $connstring = "dbi:Firebird:db=$db{name}";
    $db{host} ||= ''; $db{port} ||= ''; $db{pass} ||= '';
    length $db{host} and $connstring .= ";host=$db{host}";
    length $db{port} and $connstring .= ";port=$db{port}";
    length $db{conn} and $connstring .= ";$db{conn}";

    return "$dbtype\n$connstring\n$db{user}\n$db{pass}";

} ## end firebird

if ($dbtype eq 'oracle') {

    ## We should loosen this up somewhere
    length $db{name} or elog(ERROR, qq{Database name is mandatory\n});
    length $db{user} or elog(ERROR, qq{Database username is mandatory\n});

    ## TODO: Support SID, other forms
    my $connstring = "dbi:Oracle:dbname=$db{name}";
    $db{host} ||= ''; $db{port} ||= ''; $db{conn} ||= ''; $db{pass} ||= '';
    length $db{host} and $connstring .= ";host=$db{host}";
    length $db{port} and $connstring .= ";port=$db{port}";
    length $db{conn} and $connstring .= ";$db{conn}";

    return "$dbtype\n$connstring\n$db{user}\n$db{pass}";

} ## end oracle


if ($dbtype eq 'redis') {
   my $connstring = "$dbtype\n";
   for my $name (qw/ host port user pass name /) {
     defined $db{$name} and length $db{$name} and $connstring .= "$name: $db{$name}\n";
   }
   chomp $connstring;
   return $connstring;
}

if ($dbtype eq 'sqlite') {

    ## We should loosen this up somewhere
    length $db{name} or elog(ERROR, qq{Database name is mandatory\n});

    ## TODO: Support SID, other forms
    my $connstring = "dbi:SQLite:dbname=$db{name}";

    return "$dbtype\n$connstring";

} ## end sqlite

return "Unknown database type: $dbtype";

$bc$;


--
-- Test a database connection, and log to the db_connlog table
--
CREATE FUNCTION bucardo.db_testconn(text)
RETURNS TEXT
LANGUAGE plperlu
SECURITY DEFINER
AS
$bc$

## Given the name of a db connection, construct the connection 
## string for it and then connect to it and log the attempt

use strict; use warnings; use DBI;
my ($name, $SQL, $rv, $row, $dbh, %db, $version, $found);

$name = shift;

$name =~ s/'/''/g;
$SQL = "SELECT bucardo.db_getconn('$name') AS bob";
$rv = spi_exec_query($SQL);
if (!$rv->{processed}) {
    elog(ERROR, qq{Error: Could not find a database with an name of $name\n});
}
$row = $rv->{rows}[0]{bob};
($db{type},$db{dsn},$db{user},$db{pass}) = split /\n/ => $row;

$db{dsn} =~ s/^DSN://;

if ($db{type} ne 'postgres') {
  return '';
}

my $safeconn = "$db{dsn} user=$db{user}"; ## No password for now
$safeconn =~ s/'/''/go;
(my $safename = $name) =~ s/'/''/go;

elog(DEBUG, "Connecting as $db{dsn} user=$db{user} $$");

eval {
    $dbh = DBI->connect($db{dsn}, $db{user}, $db{pass},
        {AutoCommit=>1, RaiseError=>1, PrintError=>0});
};
if ($@ or !$dbh) {
    $SQL = "INSERT INTO bucardo.db_connlog (db,connstring,status) VALUES ('$safename','$safeconn','unknown')";
    spi_exec_query($SQL);
    return "Failed to make database connection: $@";
}

$version = $dbh->{pg_server_version};

## Just in case, switch to read/write mode
$dbh->do('SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE');

## Install plpgsql if not there already
$SQL = q{SELECT 1 FROM pg_language WHERE lanname = 'plpgsql'};
my $sth = $dbh->prepare($SQL);
my $count = $sth->execute();
$sth->finish();
if ($count < 1) {
   $dbh->do("CREATE LANGUAGE plpgsql");
}

$dbh->disconnect();

$SQL = "INSERT INTO bucardo.db_connlog (db,connstring,status,version) VALUES ('$safename','$safeconn','good',$version)";
spi_exec_query($SQL);

return "Database connection successful";

$bc$;


--
-- Check the database connection if anything changes in the db table
--
CREATE FUNCTION bucardo.db_change()
RETURNS TRIGGER
LANGUAGE plperlu
SECURITY DEFINER
AS
$bc$

return if $_TD->{new}{status} eq 'inactive';

## Test connection to the database specified
my $name = $_TD->{new}{name};
$name =~ s/'/''/g;
spi_exec_query("SELECT bucardo.db_testconn('$name')");
return;

$bc$;

CREATE TRIGGER db_change
  AFTER INSERT OR UPDATE ON bucardo.db
  FOR EACH ROW EXECUTE PROCEDURE bucardo.db_change();

--
-- Setup the goat table after any change
--
CREATE OR REPLACE FUNCTION bucardo.validate_goat()
RETURNS TRIGGER
LANGUAGE plperlu
SECURITY DEFINER
AS
$bc$

## If a row in goat has changed, re-validate and set things up for that table
elog(DEBUG, "Running validate_goat");
use strict; use warnings; use DBI;

my ($SQL, $rv, $row, %db, $dbh, $sth, $count, $oid);

my $old = $_TD->{event} eq 'UPDATE' ? $_TD->{old} : 0;
my $new = $_TD->{new};

if (!defined $new->{db}) {
   die qq{Must provide a db\n};
}
if (!defined $new->{tablename}) {
   die qq{Must provide a tablename\n};
}
if (!defined $new->{schemaname}) {
   die qq{Must provide a schemaname\n};
}

if ($new->{reltype} ne 'table') {
  return;
}

my ($dbname,$schema,$table,$pkey) =
   ($new->{db}, $new->{schemaname}, $new->{tablename}, $new->{pkey});

## Do not allow pkeytype or qpkey to be set manually.
if (defined $new->{pkeytype} and (!$old or $new->{pkeytype} ne $old->{pkeytype})) {
    die qq{Cannot set pkeytype manually\n};
}
if (defined $new->{qpkey} and (!$old or $new->{qpkey} ne $old->{qpkey})) {
    die qq{Cannot set qpkey manually\n};
}

## If this is an update, we only continue if certain fields have changed
if ($old
    and $old->{db} eq $new->{db}
    and $old->{schemaname} eq $new->{schemaname}
    and $old->{tablename} eq $new->{tablename}
    and (defined $new->{pkey} and $new->{pkey} eq $old->{pkey})
    ) {
    return;
}

(my $safedbname = $dbname) =~ s/'/''/go;
$SQL = "SELECT bucardo.db_getconn('$safedbname') AS apple";
$rv = spi_exec_query($SQL);
if (!$rv->{processed}) {
    elog(ERROR, qq{Error: Could not find a database with an name of $dbname\n});
}
$row = $rv->{rows}[0]{apple};
($db{type},$db{dsn},$db{user},$db{pass},$db{ssp}) = split /\n/ => $row;
$db{dsn} =~ s/^DSN://;

if ($db{type} ne 'postgres') {
  elog(INFO, qq{Not checking database of type $db{type}});
}

elog(DEBUG, "Connecting in validate_goat as $db{dsn} user=$db{user} pid=$$ for table $schema.$table");

$dbh = DBI->connect($db{dsn}, $db{user}, $db{pass},
    {AutoCommit=>0, RaiseError=>1, PrintError=>0});

$dbh or elog(ERROR, qq{Database connection "$db{dsn}" as user $db{user} failed: $DBI::errstr\n});

$db{ssp} or $dbh->{pg_server_prepare} = 0;

## Get column information for this table (and verify it exists)
$SQL = q{
SELECT c.oid, attnum, attname, quote_ident(attname) AS qattname, typname, atttypid
FROM   pg_attribute a, pg_type t, pg_class c, pg_namespace n
WHERE  c.relnamespace = n.oid
AND    nspname = ? AND relname = ?
AND    a.attrelid = c.oid
AND    a.atttypid = t.oid
AND    attnum > 0
};
$sth = $dbh->prepare($SQL);
$count = $sth->execute($schema,$table);
if ($count < 1) {
   $sth->finish();
   $dbh->disconnect();
   die qq{Table not found at $db{dsn}: $schema.$table\n};
}
my $col = $sth->fetchall_hashref('attnum');
$oid = $col->{each %$col}{oid};

## Find all usable unique constraints for this table
$SQL = q{
SELECT   indisprimary, indkey
FROM     pg_index i
WHERE    indisunique AND indpred IS NULL AND indexprs IS NULL AND indrelid = ?
ORDER BY indexrelid DESC
};
## DESC because we choose the "newest" index in case of a tie below
$sth = $dbh->prepare($SQL);
$count = 0+$sth->execute($oid);
my $cons = $sth->fetchall_arrayref({});
$dbh->rollback();
$dbh->disconnect();

elog(DEBUG, "Valid unique constraints found: $count\n");
if ($count < 1) {
    ## We have no usable constraints. The entries must be blank.
    my $orignew = $new->{pkey};
    $new->{pkey} = $new->{qpkey} = $new->{pkeytype} = '';

    if (!$old) { ## This was an insert: just go
        elog(DEBUG, "No usable constraints, setting pkey et. al. to blank");
        return 'MODIFY';
    }

    ## If pkey has been set to NULL, this was a specific reset request, so return
    ## If pkey ended up blank (no change, or changed to blank), just return
    if (!defined $orignew or $orignew eq '') {
        return 'MODIFY';
    }

    ## The user has tried to change it something not blank, but this is not possible.
    die qq{Cannot set pkey for table $schema.$table: no unique constraint found\n};
}

## Pick the best possible one. Primary keys are always the best choice.
my ($primary)  = grep { $_->{indisprimary} } @$cons;
my $uniq;
if (defined $primary) {# and !$old and defined $new->{pkey}) {
    $uniq = $primary;
}
else {
    my (@foo) = grep { ! $_->{indisprimary} } @$cons;
    $count = @foo;
    ## Pick the one with the smallest number of columns.
    ## In case of a tie, choose the one with the smallest column footprint
    if ($count < 2) {
        $uniq = $foo[0];
    }
    else {
        my $lowest = 10_000;
        for (@foo) {
            my $cc = $_->{indkey} =~ y/ / /;
            if ($cc < $lowest) {
                $lowest = $cc;
                $uniq = $_;
            }
        }
    }
}

## This should not happen:
if (!defined $uniq) {
   die "Could not find a suitable unique index for table $schema.$table\n";
}

my $found = 0;

## If the user is not trying a manual override, set the best one and leave
if ((!defined $new->{pkey} or !length $new->{pkey}) or ($old and $new->{pkey} eq $old->{pkey})) {
    ($new->{pkey} = $uniq->{indkey}) =~ s/(\d+)(\s+)?/$col->{$1}{attname} . ($2 ? '|' : '')/ge;
    $found = 1;
}
else {
    ## They've attempted a manual override of pkey. Make sure it is valid.
    for (@$cons) {
        (my $name = $_->{indkey}) =~ s/(\d+)(\s+)?/$col->{$1}{attname} . ($2 ? '|' : '')/ge;
        next unless $name eq $new->{pkey};
        last;
    }
}

if ($found) {
    ($new->{qpkey} = $uniq->{indkey}) =~ s/(\d+)(\s+)?/$col->{$1}{qattname} . ($2 ? '|' : '')/ge;
    ($new->{pkeytype} = $uniq->{indkey}) =~ s/(\d+)(\s+)?/$col->{$1}{typname} . ($2 ? '|' : '')/ge;
    $new->{pkeytype} =~ s/int2/smallint/;
    $new->{pkeytype} =~ s/int4/integer/;
    $new->{pkeytype} =~ s/int8/bigint/;
    return 'MODIFY';
}

die qq{Could not find a matching unique constraint that provides those columns\n};

$bc$; -- End of validate_goat()

CREATE TRIGGER validate_goat
  BEFORE INSERT OR UPDATE ON bucardo.goat
  FOR EACH ROW EXECUTE PROCEDURE bucardo.validate_goat();

--
-- Check that the goat tables are ready and compatible
--

CREATE OR REPLACE FUNCTION bucardo.validate_sync(text,integer)
RETURNS TEXT
LANGUAGE plperlu
SECURITY DEFINER
AS
$bc$

## Connect to all (active) databases used in a sync
## Verify table structures are the same
## Add delta relations as needed

use strict;
use warnings;
use DBI;

my $syncname = shift;

elog(LOG, "Starting validate_sync for $syncname");

## If force is set, we don't hesitate to drop indexes, etc.
my $force = shift || 0;

## Common vars
my ($rv,$SQL,%cache,$msg);

## Grab information about this sync from the database
(my $safesyncname = $syncname) =~ s/'/''/go;
$SQL = "SELECT * FROM sync WHERE name = '$safesyncname'";
$rv = spi_exec_query($SQL);
if (!$rv->{processed}) {
    elog(ERROR, "No such sync: $syncname");
}

my $info = $rv->{rows}[0];

## Does this herd exist?
(my $herd = $info->{herd}) =~ s/'/''/go;
$SQL = qq{SELECT 1 FROM herd WHERE name = '$herd'};
$rv = spi_exec_query($SQL);
if (!$rv->{processed}) {
    elog(ERROR, "No such relgroup: $herd");
}

## Grab information on all members of this herd
$SQL = qq{
        SELECT id, db, schemaname, tablename, pkey, pkeytype, reltype, 
               autokick AS goatkick,
               pg_catalog.quote_ident(db)         AS safedb,
               pg_catalog.quote_ident(schemaname) AS safeschema,
               pg_catalog.quote_ident(tablename)  AS safetable,
               pg_catalog.quote_ident(pkey)       AS safepkey
        FROM   goat g, herdmap h
        WHERE  g.id = h.goat
        AND    h.herd = '$herd'
    };
$rv = spi_exec_query($SQL);
if (!$rv->{processed}) {
    elog(WARNING, "Relgroup has no members: $herd");
    return qq{Herd "$herd" for sync "$syncname" has no members: cannot validate};
}

my $number_sync_relations = $rv->{processed};

## Create a simple hash so we can look up the information by schema then table name
my %goat;
for my $x (@{$rv->{rows}}) {
    $goat{$x->{schemaname}}{$x->{tablename}} = $x;
}

## Map to the actual table names used by looking at the customname table
my %customname;
$SQL = q{SELECT goat,newname,db,COALESCE(db,'') AS db, COALESCE(sync,'') AS sync FROM bucardo.customname};
$rv = spi_exec_query($SQL);
for my $x (@{$rv->{rows}}) {
    ## Ignore if this is for some other sync
    next if length $x->{sync} and $x->{sync} ne $syncname;

    $customname{$x->{goat}}{$x->{db}} = $x->{newname};
}

## Grab information from each of the databases
my %db;
(my $dbs = $info->{dbs}) =~ s/'/''/go;
$SQL = qq{
        SELECT m.db, m.role, pg_catalog.quote_ident(m.db) AS safedb, d.status, d.dbtype
        FROM   dbmap m
        JOIN   db d ON (d.name = m.db)
        WHERE  dbgroup = '$dbs'
    };
$rv = spi_exec_query($SQL);
if (!@{$rv->{rows}}) {
    elog(ERROR, qq{Could not find a dbgroup of $dbs});
}

## We also want to count up each type of role
my %role = (
    source => 0,
    target => 0,
    fullcopy => 0,
);

for (@{$rv->{rows}}) {
    $db{$_->{db}} = {
        safename => $_->{safedb},
        role => $_->{role},
        status => $_->{status},
        dbtype => $_->{dbtype},
    };
    $role{$_->{role}}++;
}

## No source databases? Does not compute!
if ($role{source} < 1) {
    die "Invalid dbgroup: at least one database must have a role of 'source'!\n";
}

## Unless we are fullcopy, we must have PKs on each table
my $is_fullcopy = (! $role{target} and $role{fullcopy}) ? 1 : 0;
if (! $is_fullcopy) {
    for my $schema (sort keys %goat) {
        for my $table (sort keys %{$goat{$schema}}) {
            next if $goat{$schema}{$table}{reltype} ne 'table';
            if (! $goat{$schema}{$table}{pkey}) {
                elog(ERROR, qq{Table "$schema.$table" must specify a primary key!});
            }
        }
    }
}

my $run_sql = sub {
    my ($sql,$dbh) = @_;
    elog(DEBUG, "SQL: $sql");
    $dbh->do($sql);
};


my $fetch1_sql = sub {
    my ($sql,$dbh,@items) = @_;
    $sql =~ s/\t/    /gsm;
    if ($sql =~ /^(\s+)/m) {
        (my $ws = $1) =~ s/[^ ]//g;
        my $leading = length($ws);
        $sql =~ s/^\s{$leading}//gsm;
    }
    my $sth = $dbh->prepare_cached($sql);
    $sth->execute(@items);
    return $sth->fetchall_arrayref()->[0][0];
};

## Determine the name of some functions we may need
my $namelen = length($syncname);
my $kickfunc = $namelen <= 48
    ? "bucardo_kick_$syncname" : $namelen <= 62
    ? "bkick_$syncname"
    : sprintf 'bucardo_kick_%d', int (rand(88888) + 11111);

## Not used yet, but will allow for selective recreation of various items below
my %force;

## Open a connection to each active database
## Create the bucardo superuser if needed
## Install the plpgsql language if needed
## We do the source ones first as all their columns must exist on all other databases
for my $dbname (sort { ($db{$b}{role} eq 'source') <=> ($db{$a}{role} eq 'source') } keys %db) {

    ## Skip if this database is not active
    next if $db{$dbname}{status} ne 'active';

    ## Skip if this is a flatfile
    next if $db{$dbname}{dbtype} =~ /flat/;

    ## Skip if this is a non-supported database
    next if $db{$dbname}{dbtype} =~ /drizzle|mariadb|mongo|mysql|oracle|redis|sqlite|firebird/;

    ## Figure out how to connect to this database
    my $rv = spi_exec_query("SELECT bucardo.db_getconn('$dbname') AS conn");
    $rv->{processed} or elog(ERROR, qq{Error: Could not find a database named "$dbname"\n});
    my ($dbtype,$dsn,$user,$pass,$ssp) = split /\n/ => $rv->{rows}[0]{conn};
    $dsn =~ s/^DSN://;
    elog(DEBUG, "Connecting to $dsn as $user inside bucardo_validate_sync for language check");
    my $dbh;
    eval {
        ## Cache this connection so we only have to connect one time
        $dbh = $cache{dbh}{$dbname} = DBI->connect
            ($dsn, $user, $pass, {AutoCommit=>0, RaiseError=>1, PrintError=>0});
    };
    if ($@) {
        ## If the error might be because the bucardo user does not exist yet,
        ## try again with the postgres user (and create the bucardo user!)
        if ($@ =~ /"bucardo"/ and $user eq 'bucardo') {
            elog(DEBUG, 'Failed connection, trying as user postgres');
            my $tempdbh = DBI->connect($dsn, 'postgres', $pass, {AutoCommit=>0, RaiseError=>1, PrintError=>0});
            $tempdbh->do('SET TRANSACTION READ WRITE');
            $tempdbh->do('CREATE USER bucardo SUPERUSER');
            $tempdbh->commit();
            $tempdbh->disconnect();

            ## Reconnect the same as above, with the new bucardo user
            $dbh = $cache{dbh}{$dbname} = DBI->connect
                ($dsn, $user, $pass, {AutoCommit=>0, RaiseError=>1, PrintError=>0});
            warn "Created superuser bucardo on database $dbname\n";
        } else {
            ## Any other connection error is a simple exception
            die $@;
        }
    }

    ## If server_side_prepares is off for this database, set it now
    $ssp or $dbh->{pg_server_prepare} = 0;

    ## Just in case this database is set to read-only
    $dbh->do('SET TRANSACTION READ WRITE');

    ## To help comparisons, remove any unknown search_paths
    $dbh->do('SET LOCAL search_path = pg_catalog');

    ## Prepare some common SQL:
    my (%sth,$sth,$count,$x,%col);

    ## Does a named schema exist?
    $SQL = q{SELECT 1 FROM pg_namespace WHERE nspname = ?};
    $sth{hazschema} = $dbh->prepare($SQL);

    ## Does a named column exist on a specific table?
    $SQL = q{SELECT 1 FROM pg_attribute WHERE attrelid = }
          .q{(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (n.oid=c.relnamespace)}
          .q{ AND nspname=? AND relname=?) AND attname = ?};
    $sth{hazcol} = $dbh->prepare($SQL);

    ## Get a list of all tables and indexes in the bucardo schema for ease below
    $SQL = q{SELECT c.oid,relkind,relname FROM pg_class c JOIN pg_namespace n ON (n.oid=c.relnamespace) WHERE nspname='bucardo'};
    $sth = $dbh->prepare($SQL);
    $sth->execute();
    my (%btableoid, %bindexoid);
    for my $row (@{$sth->fetchall_arrayref()}) {
        if ($row->[1] eq 'r') {
            $btableoid{$row->[2]} = $row->[0];
        }
        if ($row->[1] eq 'i') {
            $bindexoid{$row->[2]} = $row->[0];
        }
    }

    ## We may need to optimize some calls below for very large numbers of relations
    ## Thus, it helps to know how many this database has in total
    $sth = $dbh->prepare(q{SELECT count(*) FROM pg_class WHERE relkind IN ('r','S')});
    $sth->execute();
    my $relation_count = $sth->fetchall_arrayref()->[0][0];
 
    ## Get a list of all functions in the bucardo schema
    $SQL = q{SELECT p.oid,proname FROM pg_proc p JOIN pg_namespace n ON (n.oid=p.pronamespace) WHERE nspname='bucardo'};
    $sth = $dbh->prepare($SQL);
    $sth->execute();
    my (%bfunctionoid);
    for my $row (@{$sth->fetchall_arrayref()}) {
        $bfunctionoid{$row->[1]} = $row->[0];
    }

    ## Get a list of all triggers that start with 'bucardo'
    $SQL = q{SELECT nspname, relname, tgname FROM pg_trigger t
       JOIN pg_class c ON (c.oid=t.tgrelid)
       JOIN pg_namespace n ON (n.oid = c.relnamespace)
       WHERE tgname ~ '^bucardo'};
    $sth = $dbh->prepare($SQL);
    $sth->execute();
    my (%btriggerinfo);
    for my $row (@{$sth->fetchall_arrayref()}) {
        $btriggerinfo{$row->[0]}{$row->[1]}{$row->[2]} = 1;
    }

    ## Unless we are strictly fullcopy, put plpgsql in place on all source dbs
    ## We also will need a bucardo schema
    my $role = $db{$dbname}{role};
    if ($role eq 'source' and ! $is_fullcopy) {
        ## Perform the check for plpgsql
        $SQL = q{SELECT count(*) FROM pg_language WHERE lanname = 'plpgsql'};
        my $count = $dbh->selectall_arrayref($SQL)->[0][0];
        if ($count < 1) {
            $dbh->do('CREATE LANGUAGE plpgsql');
            $dbh->commit();
            warn "Created language plpgsql on database $dbname\n";
        }

        ## Create the bucardo schema as needed
        $sth = $sth{hazschema};
        $count = $sth->execute('bucardo');
        $sth->finish();
        if ($count < 1) {
            $dbh->do('CREATE SCHEMA bucardo');
        }
        my $newschema = $count < 1 ? 1 : 0;

my @functions = (

{ name => 'bucardo_tablename_maker', args => 'text', returns => 'text', vol => 'immutable', body => q{
DECLARE
  tname TEXT;
  newname TEXT;
  hashed TEXT;
BEGIN
  -- Change the first period to an underscore
  SELECT INTO tname REPLACE($1, '.', '_');
  -- Assumes max_identifier_length is 63
  -- Because even if not, we'll still abbreviate for consistency and portability
  SELECT INTO newname SUBSTRING(tname FROM 1 FOR 57);
  IF (newname != tname) THEN
    SELECT INTO newname SUBSTRING(tname FROM 1 FOR 46)
      || '!'
      || SUBSTRING(MD5(tname) FROM 1 FOR 10);
  END IF;
  -- We let Postgres worry about the quoting details
  SELECT INTO newname quote_ident(newname);
  RETURN newname;
END;
}
},

{ name => 'bucardo_tablename_maker', args => 'text, text', returns => 'text', vol => 'immutable', body => q{
DECLARE
  newname TEXT;
BEGIN
  SELECT INTO newname bucardo.bucardo_tablename_maker($1);

  -- If it has quotes around it, we expand the quotes to include the prefix
  IF (POSITION('"' IN newname) >= 1) THEN
    newname = REPLACE(newname, '"', '');
    newname = '"' || $2 || newname || '"';
  ELSE
    newname = $2 || newname;
  END IF;

  RETURN newname;
END;
}
},

{ name => 'bucardo_delta_names_helper', args => '', returns => 'trigger', vol => 'immutable', body => q{
BEGIN
  IF NEW.deltaname IS NULL THEN
    NEW.deltaname = bucardo.bucardo_tablename_maker(NEW.tablename, 'delta_');
  END IF;
  IF NEW.trackname IS NULL THEN
    NEW.trackname = bucardo.bucardo_tablename_maker(NEW.tablename, 'track_');
  END IF;
  RETURN NEW;
END;
}
},

## Function to do a quick check of all deltas for a given sync
{ name => 'bucardo_delta_check', args => 'text, text', returns => 'SETOF TEXT', body => q{
DECLARE
  myst TEXT;
  myrec RECORD;
  mycount INT;
BEGIN
  FOR myrec IN
    SELECT * FROM bucardo.bucardo_delta_names
      WHERE sync = $1 
      ORDER BY tablename
  LOOP

    RAISE DEBUG 'GOT % and %', myrec.deltaname, myrec.tablename;

    myst = $$
      SELECT  1
      FROM    bucardo.$$ || myrec.deltaname || $$ d
      WHERE   NOT EXISTS (
        SELECT 1
        FROM   bucardo.$$ || myrec.trackname || $$ t
        WHERE  d.txntime = t.txntime
        AND    (t.target = '$$ || $2 || $$'::text OR t.target ~ '^T:')
      ) LIMIT 1$$;
    EXECUTE myst;
    GET DIAGNOSTICS mycount = ROW_COUNT;

    IF mycount>=1 THEN
      RETURN NEXT '1,' || myrec.tablename;
    ELSE
      RETURN NEXT '0,' || myrec.tablename;
    END IF;

  END LOOP;
  RETURN;
END;
}
},

## Function to write to the tracking table upon a truncation
{ name => 'bucardo_note_truncation', args => '', returns => 'trigger', body => q{
DECLARE
  mytable TEXT;
  myst TEXT;
BEGIN
  INSERT INTO bucardo.bucardo_truncate_trigger(tablename,sname,tname,sync)
    VALUES (TG_RELID, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_ARGV[0]);

  SELECT INTO mytable
    bucardo.bucardo_tablename_maker(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, 'delta_');
  myst = 'TRUNCATE TABLE bucardo.' || mytable;
  EXECUTE myst;

  SELECT INTO mytable
    bucardo.bucardo_tablename_maker(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, 'track_');
  myst = 'TRUNCATE TABLE bucardo.' || mytable;
  EXECUTE myst;

  -- Not strictly necessary, but nice to have a clean slate
  SELECT INTO mytable
    bucardo.bucardo_tablename_maker(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, 'stage_');
  myst = 'TRUNCATE TABLE bucardo.' || mytable;
  EXECUTE myst;

  RETURN NEW;
END;
}
},

## Function to remove duplicated entries from the bucardo_delta tables
{ name => 'bucardo_compress_delta', args => 'text, text', returns => 'text', body => q{
DECLARE
  mymode TEXT;
  myoid OID;
  myst TEXT;
  got2 bool;
  drows BIGINT = 0;
  trows BIGINT = 0;
  rnames TEXT;
  rname TEXT;
  rnamerec RECORD;
  ids_where TEXT;
  ids_sel TEXT;
  ids_grp TEXT;
  idnum TEXT;
BEGIN

  -- Are we running in a proper mode?
  SELECT INTO mymode current_setting('transaction_isolation');
  IF (mymode <> 'serializable' AND mymode <> 'repeatable read') THEN
    RAISE EXCEPTION 'This function must be run in repeatable read mode';
  END IF;

  -- Grab the oid of this schema/table combo
  SELECT INTO myoid
    c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE nspname = $1 AND relname = $2;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No such table: %.%', $1, $2;
  END IF;

  ids_where = 'COALESCE(rowid,''NULL'') = COALESCE(id, ''NULL'')';
  ids_sel = 'rowid AS id';
  ids_grp = 'rowid';
  FOR rnamerec IN SELECT attname FROM pg_attribute WHERE attrelid =
    (SELECT oid FROM pg_class WHERE relname = 'bucardo_delta'
     AND relnamespace =
     (SELECT oid FROM pg_namespace WHERE nspname = 'bucardo') AND attname ~ '^rowid'
    ) LOOP
    rname = rnamerec.attname;
    rnames = COALESCE(rnames || ' ', '') || rname ;
    SELECT INTO idnum SUBSTRING(rname FROM '[[:digit:]]+');
    IF idnum IS NOT NULL THEN
      ids_where = ids_where 
      || ' AND (' 
      || rname
      || ' = id'
      || idnum
      || ' OR ('
      || rname
      || ' IS NULL AND id'
      || idnum
      || ' IS NULL))';
      ids_sel = ids_sel
      || ', '
      || rname
      || ' AS id'
      || idnum;
      ids_grp = ids_grp
      || ', '
      || rname;
    END IF;
  END LOOP;

  myst = 'DELETE FROM bucardo.bucardo_delta 
    USING (SELECT MAX(txntime) AS maxt, '||ids_sel||'
    FROM bucardo.bucardo_delta
    WHERE tablename = '||myoid||'
    GROUP BY ' || ids_grp || ') AS foo
    WHERE tablename = '|| myoid || ' AND ' || ids_where ||' AND txntime <> maxt';
  RAISE DEBUG 'Running %', myst;
  EXECUTE myst;

  GET DIAGNOSTICS drows := row_count;

  myst = 'DELETE FROM bucardo.bucardo_track'
    || ' WHERE NOT EXISTS (SELECT 1 FROM bucardo.bucardo_delta d WHERE d.txntime = bucardo_track.txntime)';
  EXECUTE myst;

  GET DIAGNOSTICS trows := row_count;

  RETURN 'Compressed '||$1||'.'||$2||'. Rows deleted from bucardo_delta: '||drows||
    ' Rows deleted from bucardo_track: '||trows;
END;
} ## end of bucardo_compress_delta body
},

{ name => 'bucardo_compress_delta', args => 'text', returns => 'text', language => 'sql', body => q{
SELECT bucardo.bucardo_compress_delta(n.nspname, c.relname)
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE relname = $1 AND pg_table_is_visible(c.oid);
}
},

{ name => 'bucardo_compress_delta', args => 'oid', returns => 'text', language => 'sql', body => q{
SELECT bucardo.bucardo_compress_delta(n.nspname, c.relname)
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.oid = $1;
}
},

## The main vacuum function to clean up the delta and track tables
{ name => 'bucardo_purge_delta_oid', 'args' => 'text, oid', returns => 'text', body => q{
DECLARE
  deltatable TEXT;
  tracktable TEXT;
  dtablename TEXT;
  myst TEXT;
  drows BIGINT = 0;
  trows BIGINT = 0;
BEGIN
  -- Store the schema and table name
  SELECT INTO dtablename
    quote_ident(nspname)||'.'||quote_ident(relname)
    FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.oid = $2;

  -- See how many dbgroups are being used by this table
  SELECT INTO drows 
    COUNT(DISTINCT target)
    FROM bucardo.bucardo_delta_targets
    WHERE tablename = $2;
  RAISE DEBUG 'delta_targets rows found for %: %', dtablename, drows;

  -- If no dbgroups, no point in going on, as we will never purge anything
  IF drows < 1 THEN
    RETURN 'Nobody is using table '|| dtablename ||', according to bucardo_delta_targets';
  END IF;

  -- Figure out the names of the delta and track tables for this relation
  SELECT INTO deltatable
    bucardo.bucardo_tablename_maker(dtablename, 'delta_');
  SELECT INTO tracktable
    bucardo.bucardo_tablename_maker(dtablename, 'track_');

  -- Delete all txntimes from the delta table that:
  -- 1) Have been used by all dbgroups listed in bucardo_delta_targets
  -- 2) Have a matching txntime from the track table
  -- 3) Are older than the first argument interval
  myst = 'DELETE FROM bucardo.'
  || deltatable
  || ' USING (SELECT txntime AS tt FROM bucardo.'
  || tracktable 
  || ' GROUP BY 1 HAVING COUNT(*) = '
  || drows
  || ') AS foo'
  || ' WHERE txntime = tt'
  || ' AND txntime < now() - interval '
  || quote_literal($1);

  EXECUTE myst;

  GET DIAGNOSTICS drows := row_count;

  -- Now that we have done that, we can remove rows from the track table
  -- which have no match at all in the delta table
  myst = 'DELETE FROM bucardo.'
  || tracktable
  || ' WHERE NOT EXISTS (SELECT 1 FROM bucardo.'
  || deltatable
  || ' d WHERE d.txntime = bucardo.'
  || tracktable
  || '.txntime)';

  EXECUTE myst;

  GET DIAGNOSTICS trows := row_count;

  RETURN 'Rows deleted from '
  || deltatable
  || ': '
  || drows
  || ' Rows deleted from '
  || tracktable
  || ': '
  || trows;

END;
} ## end of bucardo_purge_delta_oid body
},

{ name => 'bucardo_purge_delta', args => 'text', returns => 'text', body => q{
DECLARE
  myrec RECORD;
  myrez TEXT;
  total INTEGER = 0;
BEGIN

  SET LOCAL search_path = pg_catalog;

  -- Grab all potential tables to be vacuumed by looking at bucardo_delta_targets
  FOR myrec IN SELECT DISTINCT tablename FROM bucardo.bucardo_delta_targets where tablename in (select oid from pg_class where relkind='r') LOOP
    SELECT INTO myrez
      bucardo.bucardo_purge_delta_oid($1, myrec.tablename);
    RAISE NOTICE '%', myrez;
    total = total + 1;
  END LOOP;

  RETURN 'Tables processed: ' || total::text;

END;
} ## end of bucardo_purge_delta body
},

{ name => 'bucardo_purge_sync_track', args => 'text', returns => 'text', body => q{
DECLARE
  myrec RECORD;
  myst  TEXT;
BEGIN
  PERFORM 1 FROM bucardo.bucardo_delta_names WHERE sync = $1 LIMIT 1;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'No sync found named %', $1;
  END IF;

  FOR myrec IN SELECT DISTINCT tablename, deltaname, trackname
    FROM bucardo.bucardo_delta_names WHERE sync = $1
    ORDER BY tablename LOOP

    myst = 'INSERT INTO bucardo.'
    || myrec.trackname
    || ' SELECT DISTINCT txntime, '
    || quote_literal($1)
    || ' FROM bucardo.'
    || myrec.deltaname;

    RAISE DEBUG 'Running: %', myst;

    EXECUTE myst;

  END LOOP;

  RETURN 'Complete';

END;
} ## end of bucardo_purge_sync_track body
},


); ## end of %functions

   for my $info (@functions) {
       my $funcname = $info->{name};
       my ($oldmd5,$newmd5) = (0,1);
       $SQL = 'SELECT md5(prosrc), md5(?) FROM pg_proc WHERE proname=? AND oidvectortypes(proargtypes)=?';
       my $sthmd5 = $dbh->prepare($SQL);
       $count = $sthmd5->execute(" $info->{body} ", $funcname, $info->{args});
       if ($count < 1) {
           $sthmd5->finish();
       }
       else {
           ($oldmd5,$newmd5) = @{$sthmd5->fetchall_arrayref()->[0]};
       }
       if ($oldmd5 ne $newmd5) {
           my $language = $info->{language} || 'plpgsql';
           my $volatility = $info->{vol} || 'VOLATILE';
           $SQL = "
CREATE OR REPLACE FUNCTION bucardo.$funcname($info->{args})
RETURNS $info->{returns}
LANGUAGE $language
$volatility
SECURITY DEFINER
AS \$clone\$ $info->{body} \$clone\$";
           elog(DEBUG, "Writing function $funcname($info->{args})");
           $run_sql->($SQL,$dbh);
       }
   }

        ## Create the 'kickfunc' function as needed
        if (exists $bfunctionoid{$kickfunc}) {
            ## We may want to recreate this function
            if ($force{all} or $force{funcs} or $force{kickfunc}) {
                $dbh->do(qq{DROP FUNCTION bucardo."$kickfunc"()});
                delete $bfunctionoid{$kickfunc};
            }
        }

        if (! exists $bfunctionoid{$kickfunc}) {
            ## We may override this later on with a custom function from bucardo_custom_trigger
            ## and we may not even use it all, but no harm in creating the stock one here
            my $notice = $dbh->{pg_server_version} >= 90000
                ? qq{bucardo, 'kick_sync_$syncname'}
                : qq{"bucardo_kick_sync_$syncname"};
            $SQL = qq{
                  CREATE OR REPLACE FUNCTION bucardo."$kickfunc"()
                  RETURNS TRIGGER
                  VOLATILE
                  LANGUAGE plpgsql
                  AS \$notify\$
                  BEGIN
                    EXECUTE \$nn\$NOTIFY $notice\$nn\$;
                  RETURN NEW;
                  END;
                  \$notify\$;
                 };
            $run_sql->($SQL,$dbh);
        }

        ## Create the bucardo_delta_names table as needed
        if (! exists $btableoid{'bucardo_delta_names'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_delta_names (
                        sync TEXT,
                        tablename TEXT,
                        deltaname TEXT,
                        trackname TEXT,
                        cdate TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                };
            $run_sql->($SQL,$dbh);

            $SQL = qq{CREATE UNIQUE INDEX bucardo_delta_names_unique ON bucardo.bucardo_delta_names (sync,tablename)};
            $run_sql->($SQL,$dbh);

            $SQL = qq{
CREATE TRIGGER bucardo_delta_namemaker
BEFORE INSERT OR UPDATE
ON bucardo.bucardo_delta_names
FOR EACH ROW EXECUTE PROCEDURE bucardo.bucardo_delta_names_helper();
            };
            $run_sql->($SQL,$dbh);
        }

        ## Create the bucardo_delta_targets table as needed
        if (! exists $btableoid{'bucardo_delta_targets'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_delta_targets (
                        tablename  OID         NOT NULL,
                        target     TEXT        NOT NULL,
                        cdate      TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                };
            $run_sql->($SQL,$dbh);
        }

        ## Rename the target column from 'sync' as older versions used that
        $sth = $sth{hazcol};
        $count = $sth->execute('bucardo', 'bucardo_delta_targets', 'sync');
        $sth->finish();
        if (1 == $count) {
            ## Change the name!
            $SQL = qq{ALTER TABLE bucardo.bucardo_delta_targets RENAME sync TO target};
            $run_sql->($SQL,$dbh);
        }

        ## Check for missing 'target' column in the bucardo_delta_target table
        $sth = $sth{hazcol};
        $count = $sth->execute('bucardo', 'bucardo_delta_targets', 'target');
        $sth->finish();
        if ($count < 1) {
            ## As the new column cannot be null, we have to delete existing entries!
            ## However, missing this column is a pretty obscure corner-case
            $SQL = qq{DELETE FROM bucardo.bucardo_delta_targets};
            $run_sql->($SQL,$dbh);
            $SQL = qq{
                    ALTER TABLE bucardo.bucardo_delta_targets
                      ADD COLUMN target TEXT NOT NULL;
            };
            $run_sql->($SQL,$dbh);
        }

        ## Get a list of oids and relkinds for all of our goats
        ## This is much faster than doing individually
        $SQL = q{SELECT n.nspname,c.relname,relkind,c.oid FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace)};

        ## If this is a very large statement, it might be more efficient to not use a WHERE clause!
        if ($relation_count > 1000 and $number_sync_relations / $relation_count > 0.05) {
            elog(DEBUG, "Too many relations for a WHERE clause! (would ask for $number_sync_relations or $relation_count rows)");
            $sth = $dbh->prepare($SQL);
            $sth->execute();
        }
        else {
            $SQL .= ' WHERE ';
            my @args;
            for my $schema (sort keys %goat) {
                for my $table (sort keys %{$goat{$schema}}) {
                    $SQL .= '(nspname = ? AND relname = ?) OR ';
                    push @args => $schema, $table;
                }
            }
            $SQL =~ s/OR $//;
            $sth = $dbh->prepare($SQL);
            $sth->execute(@args);
        }
 
        my %tableoid;
        my %sequenceoid;
        for my $row (@{$sth->fetchall_arrayref()}) {
            if ($row->[2] eq 'r') {
                $tableoid{"$row->[0].$row->[1]"} = $row->[3];
            }
            if ($row->[2] eq 'S') {
                $sequenceoid{"$row->[0].$row->[1]"} = $row->[3];
            }
        }

        ## Grab all the information inside of bucardo_delta_targets
        my $targetname = "dbgroup $info->{dbs}";
        $SQL = 'SELECT DISTINCT tablename FROM bucardo.bucardo_delta_targets WHERE target = ?';
        $sth = $dbh->prepare($SQL);
        $sth->execute($targetname);
        my $targetoid = $sth->fetchall_hashref('tablename');

        ## Populate bucardo_delta_targets with this dbgroup name
        $SQL = 'INSERT INTO bucardo.bucardo_delta_targets(tablename,target) VALUES (?,?)';
        my $stha = $dbh->prepare($SQL);
        for my $schema (sort keys %goat) {
            for my $table (sort keys %{$goat{$schema}}) {
                next if ! exists $tableoid{"$schema.$table"};
                my $oid = $tableoid{"$schema.$table"};
                next if exists $targetoid->{$oid};
                $stha->execute($oid, $targetname);
            }
        }

        ## Delete any tables that are no longer in the database.
        $dbh->do(q{
            DELETE FROM bucardo.bucardo_delta_targets
             WHERE NOT EXISTS (SELECT oid FROM pg_class WHERE oid = tablename)
        });

        ## Create the bucardo_truncate_trigger table as needed
        if (! exists $btableoid{'bucardo_truncate_trigger'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_truncate_trigger (
                        tablename   OID         NOT NULL,
                        sname       TEXT        NOT NULL,
                        tname       TEXT        NOT NULL,
                        sync        TEXT        NOT NULL,
                        replicated  TIMESTAMPTZ     NULL,
                        cdate       TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                };
            $run_sql->($SQL,$dbh);

            $SQL = q{CREATE INDEX bucardo_truncate_trigger_index ON }
                . q{bucardo.bucardo_truncate_trigger (sync, tablename) WHERE replicated IS NULL};
            $run_sql->($SQL,$dbh);
        }

        ## Create the bucardo_truncate_trigger_log table as needed
        if (! exists $btableoid{'bucardo_truncate_trigger_log'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_truncate_trigger_log (
                        tablename   OID         NOT NULL,
                        sname       TEXT        NOT NULL,
                        tname       TEXT        NOT NULL,
                        sync        TEXT        NOT NULL,
                        target      TEXT        NOT NULL,
                        replicated  TIMESTAMPTZ NOT NULL,
                        cdate       TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                };
            $run_sql->($SQL,$dbh);
        }

        if (exists $btableoid{'bucardo_sequences'}) {
            ## Check for older version of bucardo_sequences table
            $SQL = q{SELECT count(*) FROM pg_attribute WHERE attname = 'targetname' }
                  .q{ AND attrelid = (SELECT c.oid FROM pg_class c, pg_namespace n }
                  .q{ WHERE n.oid = c.relnamespace AND n.nspname = 'bucardo' }
                  .q{ AND c.relname = 'bucardo_sequences')};
            if ($dbh->selectall_arrayref($SQL)->[0][0] < 1) {
                warn "Dropping older version of bucardo_sequences, then recreating empty\n";
                $dbh->do('DROP TABLE bucardo.bucardo_sequences');
                delete $btableoid{'bucardo_sequences'};
            }
        }
        if (! exists $btableoid{'bucardo_sequences'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_sequences (
                        schemaname   TEXT        NOT NULL,
                        seqname      TEXT        NOT NULL,
                        syncname     TEXT        NOT NULL,
                        targetname   TEXT        NOT NULL,
                        last_value   BIGINT      NOT NULL,
                        start_value  BIGINT      NOT NULL,
                        increment_by BIGINT      NOT NULL,
                        max_value    BIGINT      NOT NULL,
                        min_value    BIGINT      NOT NULL,
                        is_cycled    BOOL        NOT NULL,
                        is_called    BOOL        NOT NULL
                    );
                };
            $run_sql->($SQL,$dbh);

            $SQL = q{CREATE UNIQUE INDEX bucardo_sequences_tablename ON }
                . q{bucardo.bucardo_sequences (schemaname, seqname, syncname, targetname)};
            $run_sql->($SQL,$dbh);
        }

    } ## end not fullcopy / all global items

    ## Build another list of information for each table
    ## This saves us multiple lookups
    $SQL = q{SELECT n.nspname,c.relname,relkind,c.oid FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace) WHERE };
    my $SQL2 = q{INSERT INTO bucardo.bucardo_delta_names VALUES };

    my (@args,@tablelist);

    for my $schema (sort keys %goat) {
        for my $table (sort keys %{$goat{$schema}}) {

            ## Map to the actual table name used, via the customname table
            my ($remoteschema,$remotetable) = ($schema,$table);

            ## The internal ID for this table
            my $id = $goat{$schema}{$table}{id};

            ## Is this a source or target database?
            ## Only pure targets can have a customname
            my $is_target = $role eq 'target';

            if ($is_target and exists $customname{$id}) {
                ## If there is an entry for this particular database, use that
                ## Otherwise, if there is a database-wide one, use that
                if (exists $customname{$id}{$dbname} or exists $customname{$id}{''}) {
                    $remotetable = $customname{$id}{$dbname} || $customname{$id}{''};

                    ## If this has a dot, change the schema as well
                    ## Otherwise, we simply use the existing schema
                    if ($remotetable =~ s/(.+)\.//) {
                        $remoteschema = $1;
                    }
                }
            }

            $SQL .= '(nspname = ? AND relname = ?) OR ';
            push @args => $remoteschema, $remotetable;
            if ($goat{$schema}{$table}{reltype} eq 'table') {
                push @tablelist => $syncname, $remoteschema, $remotetable;
            }

        } ## end each table

    } ## end each schema

    $SQL =~ s/OR $//;

    $sth = $dbh->prepare($SQL);
    $sth->execute(@args);

    my (%goatoid,@tableoids);
    for my $row (@{$sth->fetchall_arrayref()}) {
        $goatoid{"$row->[0].$row->[1]"} = [$row->[2],$row->[3]];
        push @tableoids => $row->[3] if $row->[2] eq 'r';
    }

    ## Populate the bucardo_delta_names table for this sync
    if ($role eq 'source' and ! $is_fullcopy and @tablelist) {
        $SQL = 'DELETE FROM bucardo.bucardo_delta_names WHERE sync = ?';
        $sth = $dbh->prepare($SQL);
        $sth->execute($syncname);
        $SQL = $SQL2;
        my $number = @tablelist / 3;
        $SQL .= q{(?,quote_ident(?)||'.'||quote_ident(?)),} x $number;
        chop $SQL;
        $sth = $dbh->prepare($SQL);
        $sth->execute(@tablelist);
    }
   
    ## Get column information about all of our tables
    $SQL = q{
            SELECT   attrelid, attname, quote_ident(attname) AS qattname, atttypid, format_type(atttypid, atttypmod) AS ftype,
                     attnotnull, atthasdef, attnum,
                     (SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid=attrelid
                      AND adnum=attnum AND atthasdef) AS def
            FROM     pg_attribute
            WHERE    attrelid IN (COLIST) AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum
        };
    my $columninfo;
    if (@tableoids) {
        $SQL =~ s/COLIST/join ',' => @tableoids/e;
        $sth = $dbh->prepare($SQL);
        $sth->execute();
        for my $row (@{ $sth->fetchall_arrayref({}) }) {
            my $oid = $row->{attrelid};
            $columninfo->{$oid}{$row->{attname}} = $row;
        }
    }

    ## Check out each table in turn

  SCHEMA: for my $schema (sort keys %goat) {

        ## Does this schema exist?
        $sth = $sth{hazschema};
        $count = $sth->execute($schema);
        $sth->finish();
        if ($count < 1) {
            die qq{Could not find schema "$schema" in database "$dbname"!\n};
        }

      TABLE: for my $table (sort keys %{$goat{$schema}}) {

         ## Map to the actual table name used, via the customname table
         my ($remoteschema,$remotetable) = ($schema,$table);

         ## The internal ID for this table
         my $id = $goat{$schema}{$table}{id};

         ## Is this a source or target database?
         ## Only pure targets can have a customname
         my $is_target = $role eq 'target';

         if ($is_target and exists $customname{$id}) {
           ## If there is an entry for this particular database, use that
           ## Otherwise, if there is a database-wide one, use that
           if (exists $customname{$id}{$dbname} or exists $customname{$id}{''}) {
             $remotetable = $customname{$id}{$dbname} || $customname{$id}{''};

             ## If this has a dot, change the schema as well
             ## Otherwise, we simply use the existing schema
             if ($remotetable =~ s/(.+)\.//) {
               $remoteschema = $1;
             }
           }
         }

         if (! exists $goatoid{"$remoteschema.$remotetable"}) {
             die qq{Could not find "$remotetable" inside the "$remoteschema" schema on database "$dbname"!\n};
         }
         my ($relkind,$oid) = @{ $goatoid{"$remoteschema.$remotetable"} };

         ## Verify that this is the kind of relation we expect it to be
         my $tinfo = $goat{$schema}{$table};
         if ('r' eq $relkind) {
             if ('table' ne $tinfo->{reltype}) {
                 die qq{Found "$remoteschema.$remotetable" on database "$dbname", but it's a table, not a $tinfo->{reltype}!};
             }
         }
         elsif ('S' eq $relkind) {
             if ('sequence' ne $tinfo->{reltype}) {
                 die qq{Found "$remoteschema.$remotetable" on database "$dbname", but it's a sequence, not a $tinfo->{reltype}!};
             }
         }
         else {
             die qq{Found "$remoteschema.$remotetable" on database "$dbname", but it's neither a table nor a sequence!};
         }

         ## Nothing further needed if it's a sequence
         next TABLE if $tinfo->{reltype} eq 'sequence';

         ## Get the escaped version of things
         my $safeschema = $tinfo->{safeschema};
         my $safetable = $tinfo->{safetable};

         ## Go through each column in the tables to check against the other databases

         if (! exists $columninfo->{$oid}) {
             $sth->finish();
             die qq{Could not determine column information for table "$remoteschema.$remotetable"!\n};
         }

         my $colinfo = $columninfo->{$oid};
            ## Allow for 'dead' columns in the attnum ordering
            ## Turn the old keys (attname) into new keys (number)
            $x=1;
            for (sort { $colinfo->{$a}{attnum} <=> $colinfo->{$b}{attnum} } keys %$colinfo) {
                $colinfo->{$_}{realattnum} = $x++;
            }

            ## Things that will cause it to fail this sync
            my @problem;

            ## Things that are problematic but not a show-stopper
            my @warning;

            ## Is this the first time we've seen this table?
            ## If so, this becomes canonical entry
            my $t = "$schema.$table";
            if (! exists $col{$t}) {
                $col{$t} = $colinfo; ## hashref: key is column name
                $col{db} = $dbname;
            }
            else { ## Seen this before, so check against canonical list

                ## First, any columns that exist on a source but not this one is not allowed
                for my $c1 (sort keys %{$col{$t}}) {
                    if (! exists $colinfo->{$c1}) {
                        push @problem => "Column $t.$c1 exists on db $col{db} but not on db $dbname";
                    }
                }

                ## Any columns that exist here but not the original source may be a problem
                for my $c2 (sort keys %$colinfo) {
                    if (! exists $col{$t}{$c2}) {
                        my $msg = "Column $t.$c2 exists on db $dbname but not on db $col{db}";
                        if ($role eq 'source') {
                            push @problem => $msg;
                        } else {
                            push @warning => $msg;
                        }
                        next;    ## Skip to next column
                    }
                    my $c1 = $col{$t}{$c2};

                    ## Must be in the same order so we can COPY smoothly
                    ## Someday we can consider using a custom COPY list if the server supports it
                    if ($c1->{realattnum} != $c2->{realattnum}) {
                        push @problem => "Column $t.$c1 is in position $c2->{realattnum} on db $col{db}"
                            . " but in position $c1->{realattnum} on db $dbname";
                    }

                    ## Must be the same (or very compatible) datatypes
                    if ($c1->{ftype} ne $c2->{ftype}) {
                        $msg = "Column $t.$c1 is type $c1->{ftype} on db $col{db} but type $c2->{ftype} on db $dbname";
                                ## Carve out some known exceptions (but still warn about them)
                                ## Allowed: varchar == text
                        if (($c1->{ftype} eq 'character varying' and $c2->{ftype} eq 'text') or
                                ($c2->{ftype} eq 'character varying' and $c1->{ftype} eq 'text')) {
                            push @warning => $msg;
                        } else {
                            push @problem => $msg;
                        }
                    }

                    ## Warn of a notnull mismatch
                    if ($c1->{attnotnull} != $c2->{attnotnull}) {
                        push @warning => sprintf 'Column %s on db %s is %s but %s on db %s',
                            "$t.$c1", $col{db},
                                $c1->{attnotnull} ? 'NOT NULL' : 'NULL',
                                    $c2->{attnotnull} ? 'NOT NULL' : 'NULL',
                                        $dbname;
                    }

                    ## Warn of DEFAULT existence mismatch
                    if ($c1->{atthasdef} != $c2->{atthasdef}) {
                        push @warning => sprintf 'Column %s on db %s %s but %s on db %s',
                            "$t.$c1", $col{db},
                                $c1->{atthasdef} ? 'has a DEFAULT value' : 'has no DEFAULT value',
                                    $c2->{attnotnull} ? 'has none' : 'does',
                                        $dbname;
                    }

                }                ## end each column to check

            }              ## end check this against previous source db

            if (@problem) {
                $msg = "Column verification failed:\n";
                $msg .= join "\n" => @problem;
                die $msg;
            }

            if (@warning) {
                $msg = "Warnings found on column verification:\n";
                $msg .= join "\n" => @warning;
                warn $msg;
            }

            ## If this is not a source database, we don't need to go any further
            next if $role ne 'source';

            ## If this is a fullcopy only sync, also don't need to go any further
            next if $is_fullcopy;

            ## This is a source database and we need to track changes.
            ## First step: a way to add things to the bucardo_delta table

            ## We can only put a truncate trigger in if the database is 8.4 or higher
            if ($dbh->{pg_server_version} >= 80400) {
                ## Figure out the name of this trigger
                my $trunctrig = $namelen <= 42
                    ? "bucardo_note_trunc_$syncname" : $namelen <= 54
                        ? "btrunc_$syncname"
                            : sprintf 'bucardo_note_trunc_%d', int (rand(88888) + 11111);
                if (! exists $btriggerinfo{$schema}{$table}{$trunctrig}) {
                    $SQL = qq{
          CREATE TRIGGER "$trunctrig"
          AFTER TRUNCATE ON "$schema"."$table"
          FOR EACH STATEMENT EXECUTE PROCEDURE bucardo.bucardo_note_truncation('$syncname')
        };
                    $run_sql->($SQL,$dbh);
                }
            }

            $SQL = "SELECT bucardo.bucardo_tablename_maker(?)";
            my $makername = $fetch1_sql->($SQL,$dbh,$schema.'_'.$table);
            ## Create this table if needed, with one column per PK columns
            my $delta_table = "delta_$makername";
            my $index1_name = "dex1_$makername";
            my $index2_name = "dex2_$makername";
            my $deltafunc = "delta_$makername";
            my $track_table = "track_$makername";
            my $index3_name = "dex3_$makername";
            my $stage_table = "stage_$makername";
            ## Need to account for quoted versions, e.g. names with spaces
            if ($makername =~ s/"//g) {
              $delta_table = qq{"delta_$makername"};
              $index1_name = qq{"dex1_$makername"};
              $index2_name = qq{"dex2_$makername"};
              $deltafunc = qq{"delta_$makername"};
              $track_table = qq{"track_$makername"};
              $index3_name = qq{"dex3_$makername"};
              $stage_table = qq{"stage_$makername"};
            }
            ## Also need non-quoted versions to feed to execute()
            (my $noquote_delta_table = $delta_table) =~ s/^"(.+)"$/$1/;
            (my $noquote_index1_name = $index1_name) =~ s/^"(.+)"$/$1/;
            (my $noquote_index2_name = $index2_name) =~ s/^"(.+)"$/$1/;
            (my $noquote_deltafunc = $deltafunc) =~ s/^"(.+)"$/$1/;
            (my $noquote_track_table = $track_table) =~ s/^"(.+)"$/$1/;
            (my $noquote_index3_name = $index3_name) =~ s/^"(.+)"$/$1/;
            (my $noquote_stage_table = $stage_table) =~ s/^"(.+)"$/$1/;

            if (! exists $btableoid{$noquote_delta_table}) {
               ## Create that table!
               my $pkcols = join ',' => map { qq{"$_"} } split (/\|/ => $tinfo->{pkey});
               $SQL = qq{
                   CREATE TABLE bucardo.$delta_table
                     AS SELECT $pkcols, now()::TIMESTAMPTZ AS txntime
                        FROM "$schema"."$table" LIMIT 0
               };
               $run_sql->($SQL,$dbh);
               $SQL = qq{
                   ALTER TABLE bucardo.$delta_table
                     ALTER txntime SET NOT NULL,
                     ALTER txntime SET DEFAULT now()
               };
               $run_sql->($SQL, $dbh);
            }

            ## Need an index on the txntime column
            if (! exists $bindexoid{$noquote_index1_name}) {
                $SQL = qq{CREATE INDEX $index1_name ON bucardo.$delta_table(txntime)};
                $run_sql->($SQL, $dbh);
            }

            ## Need an index on all other columns
            if (! exists $bindexoid{$noquote_index2_name}) {
                my $pkcols = join ',' => map { qq{"$_"} } split (/\|/ => $tinfo->{pkey});
                $SQL = qq{CREATE INDEX $index2_name ON bucardo.$delta_table($pkcols)};
                $run_sql->($SQL, $dbh);
            }

            ## Track any change (insert/update/delete) with an entry in bucardo_delta

            ## Trigger function to add any changed primary key rows to this new table
            ## TODO: Check for too long of a name
            ## Function is same as the table name?

            my @pkeys = split (/\|/ => $tinfo->{pkey});

         if (! exists $bfunctionoid{$noquote_deltafunc}) {
                 my $new = join ',' => map { qq{NEW."$_"} } @pkeys;
                 my $old = join ',' => map { qq{OLD."$_"} } @pkeys;
                 my $clause = join ' OR ' => map { qq{OLD."$_" <> NEW."$_"} } @pkeys;
                $SQL = qq{
        CREATE OR REPLACE FUNCTION bucardo.$deltafunc()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        SECURITY DEFINER
        VOLATILE
        AS
        \$clone\$
        BEGIN
        IF (TG_OP = 'INSERT') THEN
          INSERT INTO bucardo.$delta_table VALUES ($new);
        ELSIF (TG_OP = 'UPDATE') THEN
          INSERT INTO bucardo.$delta_table VALUES ($old);
          IF ($clause) THEN
            INSERT INTO bucardo.$delta_table VALUES ($new);
          END IF;
        ELSE
          INSERT INTO bucardo.$delta_table VALUES ($old);
        END IF;
        RETURN NULL;
        END;
        \$clone\$;
      };
                $run_sql->($SQL,$dbh);
            }

            ## Check if the bucardo_delta is a custom function, and create if needed
            $SQL = qq{SELECT trigger_language,trigger_body FROM bucardo_custom_trigger 
              WHERE goat=$tinfo->{id} 
              AND status='active' 
              AND trigger_type='delta'
    };
            elog(DEBUG, "Running $SQL");
            $rv = spi_exec_query($SQL);
            my $customdeltafunc = '';
            if ($rv->{processed}) {
                my $customdeltafunc = "bucardo_delta_$tinfo->{id}";

                if (! exists $bfunctionoid{$customdeltafunc}) {
                    $SQL = qq{
               CREATE OR REPLACE FUNCTION bucardo."$customdeltafunc"()
               RETURNS TRIGGER
               LANGUAGE $rv->{rows}[0]{trigger_language}
               SECURITY DEFINER
               VOLATILE
               AS
               \$clone\$
         };
                    $SQL .= qq{ $rv->{rows}[0]{trigger_body} };
                    $SQL .= qq{ \$clone\$; };
                    $run_sql->($SQL,$dbh);
                }
            }

            if (! exists $btriggerinfo{$schema}{$table}{'bucardo_delta'}) {
                my $func = $customdeltafunc || $deltafunc;
                $SQL = qq{
        CREATE TRIGGER bucardo_delta
        AFTER INSERT OR UPDATE OR DELETE ON "$schema"."$table"
        FOR EACH ROW EXECUTE PROCEDURE bucardo.$func()
      };
                $run_sql->($SQL,$dbh);
            }


            ## Now the 'track' table
            if (! exists $btableoid{$noquote_track_table}) {
                $SQL = qq{
                   CREATE TABLE bucardo.$track_table (
                      txntime    TIMESTAMPTZ,
                      target     TEXT
                   );
                };
                $run_sql->($SQL,$dbh);
            }

            ## Need to index both columns of the txntime table
            if (! exists $bindexoid{$noquote_index3_name}) {
                $SQL = qq{CREATE INDEX $index3_name ON bucardo.$track_table(target text_pattern_ops, txntime)};
                $run_sql->($SQL,$dbh);
            }

            ## The 'stage' table, which feeds 'track' once targets have committed
            if (! exists $btableoid{$noquote_stage_table}) {
                my $unlogged = $dbh->{pg_server_version} >= 90100 ? 'UNLOGGED' : '';
                $SQL = qq{
                   CREATE $unlogged TABLE bucardo.$stage_table (
                      txntime    TIMESTAMPTZ,
                      target     TEXT
                   );
                };
                $run_sql->($SQL,$dbh);
            }

            my $indexname = 'bucardo_delta_target_unique';
            if (! exists $bindexoid{$indexname}) {
                $dbh->do(qq{CREATE INDEX $indexname ON bucardo.bucardo_delta_targets(tablename,target)});
                $bindexoid{$indexname} = 1;
            }

            ## Override the 'autokick' kick trigger if needed
            $SQL = qq{SELECT trigger_language,trigger_body,trigger_level FROM bucardo_custom_trigger
              WHERE goat=$tinfo->{id}
              AND status='active'
              AND trigger_type='triggerkick'
            };
            elog(DEBUG, "Running $SQL");
            $rv = spi_exec_query($SQL);
            if ($rv->{processed}) {
                my $custom_function_name = "bucardo_triggerkick_$tinfo->{id}";
                if (! exists $bfunctionoid{$custom_function_name}) {
                    my $custom_trigger_level = $rv->{rows}[0]{trigger_level};
                    $SQL = qq{
               CREATE OR REPLACE FUNCTION bucardo."$custom_function_name"()
               RETURNS TRIGGER 
               LANGUAGE $rv->{rows}[0]{trigger_language}
               AS \$notify\$
        };
                    $SQL .= qq{ $rv->{rows}[0]{trigger_body} };
                    $SQL .= qq{ \$notify\$; };
                }
            }

            ## Add in the autokick triggers as needed
            ## Skip if autokick is false
            if ($info->{autokick} eq 'f') {
                if (exists $btriggerinfo{$schema}{$table}{$kickfunc}) {
                    $SQL = qq{DROP TRIGGER "$kickfunc" ON $safeschema.$safetable};
                    ## This is important enough that we want to be verbose about it:
                    warn "Dropped trigger $kickfunc from table $safeschema.$safetable\n";
                    $run_sql->($SQL,$dbh);
                    delete $btriggerinfo{$schema}{$table}{$kickfunc};
                }
                next TABLE;
            }
            if (! exists $btriggerinfo{$schema}{$table}{$kickfunc}) {
                my $ttrig = $dbh->{pg_server_version} >= 80400 ? ' OR TRUNCATE' : '';
                my $custom_trigger_level = '';
                my $custom_function_name = '';
                if ($custom_trigger_level && $custom_function_name) {
                    $SQL = qq{
                    CREATE TRIGGER "$kickfunc" FIXMENAME
                    AFTER INSERT OR UPDATE OR DELETE$ttrig ON $safeschema.$safetable
                    FOR EACH $custom_trigger_level EXECUTE PROCEDURE bucardo."$custom_function_name"()
                    };
                }
                else {
                    $SQL = qq{
                    CREATE TRIGGER "$kickfunc"
                    AFTER INSERT OR UPDATE OR DELETE$ttrig ON $safeschema.$safetable
                    FOR EACH STATEMENT EXECUTE PROCEDURE bucardo."$kickfunc"()
                    };
                }
                $run_sql->($SQL,$dbh);

            }
        } ## end each TABLE
    }     ## end each SCHEMA

    $dbh->commit();

}         ## end connecting to each database

## Gather information from bucardo_config
my $config;
$SQL = 'SELECT name,setting FROM bucardo_config';
$rv = spi_exec_query($SQL);
for my $row (@{$rv->{rows}}) {
    $config->{$row->{setting}} = $row->{value};
}


## Update the bucardo_delta_targets table as needed
## FIXME FROM old
#if ($info->{synctype} eq 'swap') {
    ## Add source to the target(s)
    ## MORE FIXME
#}

## Disconnect from all our databases
for (values %{$cache{dbh}}) {
    $_->disconnect();
}

## Let anyone listening know that we just finished the validation
$SQL = qq{NOTIFY "bucardo_validated_sync_$syncname"};
spi_exec_query($SQL);

elog(LOG, "Ending validate_sync for $syncname");

return 'MODIFY';

$bc$;
-- end of validate_sync

CREATE OR REPLACE FUNCTION bucardo.validate_sync(text)
RETURNS TEXT
LANGUAGE SQL
AS
$bc$
 SELECT bucardo.validate_sync($1,0);
$bc$;

CREATE OR REPLACE FUNCTION bucardo.validate_all_syncs(integer)
RETURNS INTEGER
LANGUAGE plpgsql
AS
$bc$
DECLARE count INTEGER = 0; myrec RECORD;
BEGIN
FOR myrec IN SELECT name FROM sync ORDER BY name LOOP
  PERFORM bucardo.validate_sync(myrec.name, $1);
  count = count + 1;
END LOOP;
RETURN count;
END;
$bc$;

CREATE OR REPLACE FUNCTION bucardo.validate_all_syncs()
RETURNS INTEGER
LANGUAGE SQL
AS
$bc$
SELECT bucardo.validate_all_syncs(0);
$bc$;

CREATE FUNCTION bucardo.validate_sync()
RETURNS TRIGGER
LANGUAGE plperlu
SECURITY DEFINER
AS $bc$

    use strict; use warnings;

    elog(DEBUG, "Starting validate_sync trigger");
    my $new = $_TD->{new};
    my $found=0;

    ## If insert, we always do the full validation:

    if ($_TD->{event} eq 'INSERT') {
        elog(DEBUG, "Found insert, will call validate_sync");
        $found = 1;
    }
    else {
        my $old = $_TD->{old};
        for my $x (qw(name herd dbs autokick)) {
            elog(DEBUG, "Checking on $x");
            if (! defined $old->{$x}) {
                next if ! defined $new->{$x};
            }
            elsif (defined $new->{$x} and $new->{$x} eq $old->{$x}) {
                next;
            }
            $found=1;
            last;
        }
    }
    if ($found) {
        spi_exec_query("SELECT validate_sync('$new->{name}')");
    }
    return;
$bc$;

CREATE TRIGGER validate_sync
  AFTER INSERT OR UPDATE ON bucardo.sync
  FOR EACH ROW EXECUTE PROCEDURE bucardo.validate_sync();

CREATE FUNCTION bucardo.bucardo_delete_sync()
RETURNS TRIGGER
LANGUAGE plperlu
SECURITY DEFINER
AS $bc$

    use strict; use warnings;

    elog(DEBUG, "Starting delete_sync trigger");

    my $old = $_TD->{old};

    my ($SQL, $rv, $sth, $count);

    ## Gather up a list of tables used in this sync, as well as the source database handle

    (my $herd = $old->{herd}) =~ s/'/''/go;

    ## Does this herd exist?
    $SQL = qq{SELECT 1 FROM herd WHERE name = '$herd'};
    $rv = spi_exec_query($SQL);
    if (!$rv->{processed}) {
       #elog(ERROR, "Cannot delete: sync refers to an invalid relgroup: $herd");
    }

    $SQL = qq{
        SELECT db, pg_catalog.quote_ident(schemaname) AS safeschema,
                   pg_catalog.quote_ident(tablename)  AS safetable
        FROM   goat g, herdmap h
        WHERE  g.id = h.goat
        AND    h.herd = '$herd'
    };
    $rv = spi_exec_query($SQL);
    if (!$rv->{processed}) {
      elog(DEBUG, 'Relgroup has no members, so no further work needed');
      return;
    }

    ## TODO: Reach out and clean up remote databases as before if needed

    return;

$bc$;

CREATE TRIGGER bucardo_delete_sync
  AFTER DELETE ON bucardo.sync
  FOR EACH ROW EXECUTE PROCEDURE bucardo.bucardo_delete_sync();


CREATE OR REPLACE FUNCTION bucardo.find_unused_goats()
RETURNS SETOF text
LANGUAGE plpgsql
AS $bc$
DECLARE
  myrec RECORD;
BEGIN
  FOR myrec IN 
    SELECT quote_ident(db) || '.' || quote_ident(schemaname) || '.' || quote_ident(tablename) AS t
      FROM goat g
      WHERE NOT EXISTS (SELECT 1 FROM herdmap h WHERE h.goat = g.id)
      ORDER BY schemaname, tablename
    LOOP
      RETURN NEXT 'Not used in any herds: ' || myrec.t;
  END LOOP;

  FOR myrec IN 
    SELECT quote_ident(db) || '.' || quote_ident(schemaname) || '.' || quote_ident(tablename) AS t
      FROM goat g
      JOIN herdmap h ON h.goat = g.id
      WHERE NOT EXISTS (SELECT 1 FROM sync WHERE source = h.herd)
      ORDER BY schemaname, tablename
    LOOP
      RETURN NEXT 'Not used in source herd: ' || myrec.t;
  END LOOP;

  FOR myrec IN 
    SELECT quote_ident(db) || '.' || quote_ident(schemaname) || '.' || quote_ident(tablename) AS t
      FROM goat g
      JOIN herdmap h ON h.goat = g.id
      WHERE NOT EXISTS (SELECT 1 FROM sync WHERE source = h.herd AND status = 'active')
      ORDER BY schemaname, tablename
    LOOP
      RETURN NEXT 'Not used in source herd of active sync: ' || myrec.t;
  END LOOP;

  RETURN;
END;
$bc$;

-- Monitor how long data takes to move over, from commit to commit
CREATE TABLE bucardo.bucardo_rate (
  sync         TEXT        NOT NULL,
  goat         INTEGER     NOT NULL,
  target       TEXT            NULL,
  mastercommit TIMESTAMPTZ NOT NULL,
  slavecommit  TIMESTAMPTZ NOT NULL,
  total        INTEGER     NOT NULL
);
COMMENT ON TABLE bucardo.bucardo_rate IS $$If track_rates is on, measure how fast replication occurs$$;

CREATE INDEX bucardo_rate_sync ON bucardo.bucardo_rate(sync);

-- Keep track of any upgrades as we go along
CREATE TABLE bucardo.upgrade_log (
  action   TEXT        NOT NULL,
  summary  TEXT        NOT NULL,
  version  TEXT        NOT NULL,
  cdate    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.upgrade_log IS $$Historical record of upgrade actions$$;

INSERT INTO bucardo.upgrade_log(action,summary,version)
  SELECT 'Initial install', '', setting
  FROM bucardo.bucardo_config
  WHERE name = 'bucardo_initial_version';

-- Allow users to insert messages in the Bucardo logs

CREATE FUNCTION bucardo.bucardo_log_message_notify()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $bc$
BEGIN
  EXECUTE 'NOTIFY "bucardo_log_message"';
  RETURN NULL;
END;
$bc$;

CREATE TABLE bucardo.bucardo_log_message (
  msg TEXT NOT NULL,
  cdate TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bucardo.bucardo_log_message IS $$Helper table for sending messages to the Bucardo logging system$$;

CREATE TRIGGER bucardo_log_message_trigger
  AFTER INSERT ON bucardo.bucardo_log_message
  FOR EACH STATEMENT EXECUTE PROCEDURE bucardo.bucardo_log_message_notify();

CREATE FUNCTION bucardo.magic_update()
RETURNS TEXT
LANGUAGE plpgsql
AS $bc$
DECLARE
  myver INTEGER;
BEGIN
  -- What version are we?
  SELECT INTO myver setting FROM pg_settings WHERE name = 'server_version_num';

  -- If we are 9.1 or better, change some tables to UNLOGGED
  IF myver >= 90100 THEN
    -- bucardo.dbrun: DROP, RECREATE, or SET an attribute?
  END IF; -- end of Postgres 9.1 and up

  RETURN ''::TEXT;
END;
$bc$;

SELECT bucardo.magic_update();

SELECT plperlu_test();

COMMIT;

--
-- END OF THE SCHEMA
--
