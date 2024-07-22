#!/usr/bin/env python3
"""
MySQL/MariaDB database partitioning maintenance script for Zabbix
"""
import argparse
import configparser
import logging
import logging.handlers
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# When using MySQLdb package:
# It can be installed as Debian package:
#   sudo apt install python3-mysqldb
# (it is then usable globally without separate Python virtual environment)
# or from PyPI (preferably in a virtual environment):
#   pip3 install mysqlclient
from MySQLdb import connect as mysql_connect
from MySQLdb._exceptions import Error as MySQLError

# Or, you can use mysql-connector-python from Oracle (comment out the imports above
# and uncomment these imports below)
#from mysql.connector import connect as mysql_connect
#from mysql.connector.errors import Error as MySQLError


# Otherwise, there is generally nothing to modify in the script, everything
# should be set in the configuration file.


SCRIPT_NAME = str(Path(__file__).with_suffix("").name)
CONFIG_FILE = Path(__file__).parent / f"{SCRIPT_NAME}.cfg"
ALLOWED_PERIODS: tuple[str] = ("day", "month")

logger = logging.getLogger(SCRIPT_NAME)


@dataclass
class Globals:
    current_partitions: dict
    print_only: bool


@dataclass
class Config:
    # These are all populated from the configuration file
    connect: dict
    tables: dict
    timezone: ZoneInfo
    prepare_partitions: int
    partition_name_templates: dict


def add_next_partitions(cursor, table_name: str) -> None:
    """ Adds any missing future partitions in the given table.
    """
    def get_next_name_and_date(table_name: str, counter: int) -> tuple[str, str]:
        period = Config.tables[table_name]["period"]
        dt = datetime.now(tz=Config.timezone)
        if period == "day":
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1+counter)
            lessthan_dt = dt + timedelta(days=1)
        elif period == "month":
            dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            # Increase month by 1+counter
            for _ in range(1+counter):
                dt = (dt + timedelta(days=31)).replace(day=1)
            lessthan_dt = (dt + timedelta(days=31)).replace(day=1)
        # Return tuple of new partition name and new partition "less than" date
        return (
            dt.strftime(Config.partition_name_templates[period]),
            lessthan_dt.strftime("%Y-%m-%d"),
        )

    for counter in range(Config.prepare_partitions):    # How many partitions to prepare in advance
        next_name, lessthan_date = get_next_name_and_date(table_name, counter)
        if next_name in Globals.current_partitions[table_name]:
            logger.info(f"Table '{table_name}' partition '{next_name}' already exists, ok")
        else:
            if Globals.print_only:
                logger.warning(f"Table '{table_name}' partition '{next_name}' does not exist yet, should be created")
            else:
                logger.info(f"Table '{table_name}' partition '{next_name}' does not exist yet, creating it")
                cursor.execute(
                    f"ALTER TABLE {table_name} ADD PARTITION (PARTITION {next_name} VALUES LESS THAN (UNIX_TIMESTAMP('{lessthan_date}')))"
                )


def drop_old_partitions(cursor, table_name: str) -> None:
    """ Drops partitions that are older than the "keep" period for the table.
    """
    period = Config.tables[table_name]["period"]
    keep_history = Config.tables[table_name]["keep"]
    for partition_name, lessthan_epoch in Globals.current_partitions[table_name].items():
        dt = datetime.now(tz=Config.timezone)
        if period == "day":
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=keep_history)
        elif period == "month":
            # Calculate month by month
            for _ in range(keep_history):
                dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
                dt = dt.replace(day=1)
        else:
            # Safeguard check
            logger.error(f"Invalid period '{period}' for table '{table_name}', not dropping anything")
            return
        if lessthan_epoch <= dt.timestamp():
            if Globals.print_only:
                logger.warning(f"Table '{table_name}' partition '{partition_name}' is old enough, should be dropped")
            else:
                logger.info(f"Table '{table_name}' partition '{partition_name}' is old enough, dropping it")
                cursor.execute(
                    f"ALTER TABLE {table_name} DROP PARTITION {partition_name}"
                )


def get_current_partitions(cursor, database_name: str) -> dict:
    """ Fetches and returns all current partitions in the Zabbix database.
    """
    cursor.execute("""
        SELECT table_name, partition_name, partition_description FROM information_schema.partitions
        WHERE partition_name IS NOT NULL AND table_schema = %s
        ORDER BY table_name, partition_name
        """,
        (database_name,),
    )
    partitions = {}
    tables_not_in_config = []
    for line in cursor.fetchall():
        table_name, partition_name, lessthan_epoch = line
        if table_name not in Config.tables and table_name not in tables_not_in_config:
            logger.error(f"Table '{table_name}' is partitioned but it is not listed in tables config")
            tables_not_in_config.append(table_name)     # Only report this table once
            continue
        if table_name not in partitions:
            partitions[table_name] = {}
        partitions[table_name][partition_name] = int(lessthan_epoch)
    return partitions


def read_config_file() -> None:
    """ Reads the configuration file and populates the Config dataclass.
    """
    config = configparser.ConfigParser(
        interpolation=None,
    )
    try:
        with open(CONFIG_FILE) as f:
            config.read_file(f)
    except Exception as e:
        logger.error(f"Cannot read configuration file: {e}")
        sys.exit(1)
    try:
        Config.connect = config["connect"]
        # Ensure that database name exists, for later use
        _ = Config.connect["database"]
        tables_config = config["tables"]
        misc_config = config["misc"]
        timezone_name = misc_config["timezone"]
        prepare_partitions = misc_config["prepare_partitions"]
        Config.partition_name_templates = {}
        for period in ALLOWED_PERIODS:
            Config.partition_name_templates[period] = misc_config[f"template_{period}"]
    except KeyError as e:
        error_key = e.args[0]
        logger.error(f"Cannot read {error_key} from config file {CONFIG_FILE}")
        sys.exit(1)
    try:
        Config.timezone = ZoneInfo(timezone_name)
    except:
        logger.error(f"Invalid timezone '{timezone_name}' in config file {CONFIG_FILE}")
        sys.exit(1)
    try:
        Config.prepare_partitions = int(prepare_partitions)
    except ValueError:
        logger.error(f"Invalid prepare_partitions value in config file {CONFIG_FILE}")
        sys.exit(1)
    Config.tables = {}
    for table_name, line in tables_config.items():
        try:
            try:
                period, keep_str = (v.strip().lower() for v in line.split(","))
            except ValueError:
                raise ValueError("Invalid partitioning specification (expected like: 'day, 31')") from None
            if period not in ALLOWED_PERIODS:
                raise ValueError("Invalid period")
            try:
                keep = int(keep_str)
            except ValueError:
                raise ValueError("Invalid number for keep value") from None
            Config.tables[table_name] = {
                "period": period,
                "keep": keep,
            }
        except ValueError as e:
            logger.error(f"Error reading configuration for table '{table_name}': {e}")
            sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Partition maintenance script for MySQL/MariaDB database for Zabbix")
    parser.add_argument(
        "--print-only",
        help="only print the outcome, don't make any changes to the database",
        action="store_true",
    )
    args = parser.parse_args()
    Globals.print_only = args.print_only

    logger.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    if not Globals.print_only:
        # Normally only print error on the console (to prevent cron job from emailing infos)
        console_handler.setLevel(logging.ERROR)
        syslog_formatter = logging.Formatter("%(name)s: %(levelname)s: %(message)s")
        syslog_handler = logging.handlers.SysLogHandler(address="/dev/log")
        syslog_handler.setFormatter(syslog_formatter)
        logger.addHandler(syslog_handler)

    read_config_file()
    try:
        # Use the keyword arguments as-is from the "connect" section in the config file
        db = mysql_connect(**Config.connect)
    except (MySQLError, ValueError) as e:
        logger.error(f"Error opening connection to database '{Config.connect['database']}': {e}")
        return
    try:
        cursor = db.cursor()
        Globals.current_partitions = get_current_partitions(cursor, Config.connect["database"])
        for table_name in Config.tables:
            if table_name not in Globals.current_partitions:
                logger.error(f"Partitioning for table '{table_name}' not found")
                continue
            if Config.tables[table_name]["period"] not in ALLOWED_PERIODS:
                logger.error(f"Invalid partitioning period defined for table '{table_name}' in tables config")
                continue
            add_next_partitions(cursor, table_name)
            drop_old_partitions(cursor, table_name)
            db.commit()
        # Also delete old user sessions
        if Globals.print_only:
            cursor.execute("SELECT * FROM sessions WHERE lastaccess < UNIX_TIMESTAMP(NOW() - INTERVAL 1 MONTH)")
            logger.info(f"Would have deleted {cursor.rowcount} old user sessions")
        else:
            logger.info("Deleting old user sessions")
            cursor.execute("DELETE FROM sessions WHERE lastaccess < UNIX_TIMESTAMP(NOW() - INTERVAL 1 MONTH)")
            db.commit()
    except Exception as e:
        logger.exception("Unhandled exception:", exc_info=True)
    finally:
        db.close()


if __name__ == "__main__":
    main()
