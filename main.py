import csv
from datetime import datetime
import random
import os

import numpy as np

import MySQLdb.cursors
from MySQLdb.connections import Connection

from logger import Logger, LogLevel
import queries

NUM_REPEATS_INSERT = 10
NUM_REPEATS = 200

OUTPUT_COLUMNS = ["database", "method", "max", "min", "avg", "median", "stddev"]

DATA_FILES = ["load_departments.dump", "load_employees.dump", "load_dept_emp.dump", "load_dept_manager.dump",
              "load_titles.dump", "load_salaries1.dump", "load_salaries2.dump", "load_salaries3.dump"]

MYSQL_HOST = "mysql-server"
MARIA_HOST = "mariadb-server"
DB_USER = "machina"
DB_PASS = "JestemInzynierem"
DB_NAME = "sqlperftest"
DB_CSET = "utf8"

NOW = str(int(datetime.now().timestamp()))


def do_single_query(conn: Connection, query: str):
    time_start = datetime.now()
    with conn.cursor() as cur:
        cur.execute(query)
        cur.fetchall()
    time_end = datetime.now()
    return (time_end - time_start).total_seconds()


def create_table(conn: Connection):
    with conn.cursor() as cur:
        cur.execute("SET NAMES utf8;")
        for query in queries.QUERYLIST_CREATE_STRUCTURE:
            cur.execute(query)


def insert_all(conn: Connection):
    """
    Insert all elements from [data] to database
    """
    insert_time = 0.0

    for file in DATA_FILES:
        with open(f"test_db/{file}", "r") as f:
            sql = f.read().replace("\n", " ")
            insert_time += do_single_query(conn, sql)
            conn.commit()
    return insert_time


def select_simple(conn: Connection):
    """
    Select all records from `employees` table matching simple criteria
    """
    return do_single_query(conn, queries.QUERY_SELECT_SIMPLE)


def select_sorting(conn: Connection):
    """
    Select first 5000 records from `employees` table sorted by `last_name` column values
    """
    return do_single_query(conn, queries.QUERY_SELECT_SORT)


def select_join(conn: Connection):
    """
    Select first 5000 records containing employee's first name, last name and their job title on specified date
    """
    return do_single_query(conn, queries.QUERY_SELECT_JOIN)


def select_group(conn: Connection):
    """
    Count employees working in all departments
    """
    return do_single_query(conn, queries.QUERY_SELECT_GROUP)


def select_string(conn: Connection):
    """
    Find all employees whose first and last name begin with certain letters
    """
    args = [chr(ord('A') + random.randrange(26)), chr(ord('A') + random.randrange(26))]
    return do_single_query(conn, queries.query_select_string(args))


def select_aggregates(conn: Connection):
    """
    Find multiple aggregates of salary grouped by employee and sorted descending by average salary
    """
    return do_single_query(conn, queries.QUERY_SELECT_AGGREGATE)


def do_tests_singlethread(conn: Connection, log: Logger, database: str):
    log.i(">> Begin SINGLE-threaded tests")
    conn.ping()
    output = []
    intermediate = dict()

    intermediate["insert_all"] = []
    intermediate["select_simple"] = []
    intermediate["select_sorting"] = []
    intermediate["select_join"] = []
    intermediate["select_group"] = []
    intermediate["select_string"] = []
    intermediate["select_aggregates"] = []

    for i in range(NUM_REPEATS_INSERT):
        print(f"Inserting... {i} / {NUM_REPEATS_INSERT}")
        create_table(conn)
        intermediate["insert_all"].append(insert_all(conn))
    print("Insert tests finished")

    for i in range(NUM_REPEATS):
        if i % 10 == 0:
            print(f"Querying... {i} / {NUM_REPEATS}")
        intermediate["select_simple"].append(select_simple(conn))
        intermediate["select_sorting"].append(select_sorting(conn))
        intermediate["select_join"].append(select_join(conn))
        intermediate["select_group"].append(select_group(conn))
        intermediate["select_string"].append(select_string(conn))
        intermediate["select_aggregates"].append(select_aggregates(conn))

    print("Done")

    for method in intermediate.keys():
        output.append({
            "method": method,
            "min": "%.6f" % np.amin(intermediate[method]),
            "max": "%.6f" % np.amax(intermediate[method]),
            "avg": "%.6f" % np.mean(intermediate[method]),
            "median": "%.6f" % np.median(intermediate[method]),
            "stddev": np.std(intermediate[method]),
            "database": database
        })

    return output


if __name__ == '__main__':
    # step 0 - preparations
    log = Logger(tag="SQLPerf", log_level=LogLevel.INFO)
    os.makedirs("logs/", exist_ok=True)
    log.i("----- SCRIPT STARTED -----")

    log.i("> STEP 1: test MySQL performance")
    conn_mysql = MySQLdb.connect(host=MYSQL_HOST,
                                 user=DB_USER,
                                 password=DB_PASS,
                                 db=DB_NAME,
                                 charset=DB_CSET)
    res_single_my = do_tests_singlethread(conn_mysql, log, "mysql")

    log.i("> STEP 2: test MariaDB performance")
    conn_maria = MySQLdb.connect(host=MARIA_HOST,
                                 user=DB_USER,
                                 password=DB_PASS,
                                 db=DB_NAME,
                                 charset=DB_CSET)
    res_single_ma = do_tests_singlethread(conn_maria, log, "maria")

    log.i("> STEP 3: write results")
    with open(f"logs/{NOW}-results.csv", "w") as output:
        writer = csv.DictWriter(output, OUTPUT_COLUMNS, dialect="excel")
        writer.writeheader()
        writer.writerows(res_single_my)
        writer.writerows(res_single_ma)

    log.i(f">> saved to logs/{NOW}-results.csv")

    log.i("> STEP 4: Close database connections")
    conn_mysql.close()
    conn_maria.close()
    log.i("----- SCRIPT ENDED -----")
