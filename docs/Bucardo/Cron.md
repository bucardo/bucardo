---
title: Bucardo Cron
permalink: /Bucardo/Cron/
---

**NOTE: This information is for Bucardo 4 only. If you are using version 5 or higher, none of the below is needed**

Best practices for cron jobs. A quick suggestion:

On each database that is used as a master (e.g. has a bucardo_delta table):

` */15 * * * * psql -X -q -d masterhost -U bucardo -c "SELECT`
` bucardo_purge_delta('10 minutes'::interval)"`

On the main Bucardo database:

` */30 * * * * psql -X -q -d bucardo -U bucardo -c "SELECT`
` bucardo_purge_q_table('5 minutes'::interval)"`

Once data is replicated, it gets moved from the master_q table to freezer.child_q_YYMMDD tables. If you want to keep this data around, simply do nothing, but note this will cause your bucardo database to grow. On some installations, this may negatively affect performance. If you wish to keep those tables pruned, that can be accomplished with a cron job as well. For example, you can have an /etc/cron.daily/bucardo_daily_purge file like:

` #!/bin/bash`
` echo ========BEGIN DAILY PURGE ============= >> /var/log/bucardo-purge`
` date >> /var/log/bucardo-purge`
` echo ======================================= >> /var/log/bucardo-purge`
``  PGPASSWORD=`grep dbpass /etc/bucardorc | awk -F '=' '{print $2;}'` ``
` export PGPASSWORD`
` /usr/bin/psql -U bucardo < /usr/local/share/bucardo/bucardo_daily_purge.sql >> /var/log/bucardo-purge 2>&1`
` DROP_OLD_CHILD_Q="DROP TABLE freezer.child_q_"`
``  DROP_OLD_CHILD_Q+=`/bin/date --date yesterday "+%Y%m%d"` ``
` DROP_OLD_CHILD_Q+=';' `
` echo $DROP_OLD_CHILD_Q | /usr/bin/psql -U bucardo >> /var/log/bucardo-purge 2>&1  `
` echo =========END DAILY PURGE ============== >> /var/log/bucardo-purge`

Be sure to adjust the timeframe for the date --date command above to appropriately suit your environment. If your replication takes longer than a day, for instance, the above will cause you problems. The file bucardo_daily_purge.sql in this example looks like:

` DELETE FROM q WHERE (started < now() + '1 day ago'::interval OR ended < now() + '1 day ago'::interval OR aborted < now() + '1 day ago'::interval OR cdate < now() + '1 day ago'::interval) AND (ended IS NULL OR aborted IS NULL);`

which cleans up q table entries that do not get taken care of by the bucardo_purge_q_table function.
