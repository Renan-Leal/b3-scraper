#!/bin/bash

if [ -z "$CRON_TIMER_CONF" ]; then
  echo "CRON_TIMER_CONF not set. Exiting."
  exit 1
fi

echo "$CRON_TIMER_CONF cd /app && /usr/local/bin/python main.py >> /var/log/cron.log 2>&1" > /etc/cron.d/scraper-cron

chmod 0644 /etc/cron.d/scraper-cron

crontab /etc/cron.d/scraper-cron

touch /var/log/cron.log

cron

tail -f /var/log/cron.log