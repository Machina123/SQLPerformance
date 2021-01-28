import csv
from datetime import datetime
import json
import random
import os

import numpy as np

import MySQLdb.cursors
from MySQLdb.connections import Connection

from logger import Logger, LogLevel
import queries

NUM_REPEATS_INSERT = 10
NUM_REPEATS = 100

OUTPUT_COLUMNS = ["database", "method", "max", "min", "avg", "median", "stddev"]
DETAIL_COLUMNS = ["method", "result"]

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


def create_dummy(conn: Connection):
    """
    Create dummy structure (copy data from tables without any keys or indexes)
    """
    with conn.cursor() as cur:
        cur.execute(queries.make_query_on_dummy(queries.QUERY_DROP_STRUCTURE))
        for query in queries.QUERYLIST_CREATE_DUMMY_STRUCTURE:
            cur.execute(query)
        conn.commit()


def select_simple(conn: Connection):
    """
    Select all records from `employees` table matching simple criteria
    """
    return do_single_query(conn, queries.QUERY_SELECT_SIMPLE)


def select_simple_dummy(conn: Connection):
    """
    Select all records from `dummy_employees` table matching simple criteria
    """
    return do_single_query(conn, queries.make_query_on_dummy(queries.QUERY_SELECT_SIMPLE))


def select_sorting(conn: Connection):
    """
    Select records from `employees` table sorted by `last_name` column values
    """
    return do_single_query(conn, queries.QUERY_SELECT_SORT)


def select_sorting_dummy(conn: Connection):
    """
    Select records from `employees` table sorted by `last_name` column values
    """
    return do_single_query(conn, queries.make_query_on_dummy(queries.QUERY_SELECT_SORT))


def select_join(conn: Connection):
    """
    Select first 5000 records containing employee's first name, last name and their job title on specified date
    """
    return do_single_query(conn, queries.QUERY_SELECT_JOIN)


def select_join_dummy(conn: Connection):
    """
    Select first 5000 records containing employee's first name, last name and their job title on specified date
    """
    return do_single_query(conn, queries.make_query_on_dummy(queries.QUERY_SELECT_JOIN))


def select_group(conn: Connection):
    """
    Count employees working in all departments
    """
    return do_single_query(conn, queries.QUERY_SELECT_GROUP)


def select_group_dummy(conn: Connection):
    """
    Count employees working in all departments
    """
    return do_single_query(conn, queries.make_query_on_dummy(queries.QUERY_SELECT_GROUP))


def select_string(conn: Connection, args: list):
    """
    Find all employees whose first and last name begin with certain letters
    """
    return do_single_query(conn, queries.query_select_string(args))


def select_string_dummy(conn: Connection, args: list):
    """
    Find all employees whose first and last name begin with certain letters
    """
    return do_single_query(conn, queries.make_query_on_dummy(queries.query_select_string(args)))


def select_aggregates(conn: Connection):
    """
    Find multiple aggregates of salary grouped by employee and sorted descending by average salary
    """
    return do_single_query(conn, queries.QUERY_SELECT_AGGREGATE)


def select_aggregates_dummy(conn: Connection):
    """
    Find multiple aggregates of salary grouped by employee and sorted descending by average salary
    """
    return do_single_query(conn, queries.QUERY_SELECT_AGGREGATE)


def multiply_data_in_employees(conn: Connection):
    """
    Multiply data in `employees` table
    """
    return do_single_query(conn, queries.QUERY_MULTIPLY_EMPLOYEES)


def do_tests_singlethread(conn: Connection, database: str):
    conn.ping()
    output = []
    intermediate = dict()
    select_string_args = [chr(ord('A') + random.randrange(26)), chr(ord('A') + random.randrange(26))]

    intermediate["insert_all"] = []
    intermediate["select_simple"] = []
    intermediate["select_sorting"] = []
    intermediate["select_join"] = []
    intermediate["select_group"] = []
    intermediate["select_string"] = []
    intermediate["select_aggregates"] = []

    intermediate["select_simple_dummy"] = []
    intermediate["select_join_dummy"] = []
    intermediate["select_group_dummy"] = []
    intermediate["select_string_dummy"] = []
    intermediate["select_aggregates_dummy"] = []

    for i in range(NUM_REPEATS_INSERT):
        print(f"Inserting... {i} / {NUM_REPEATS_INSERT}")
        create_table(conn)
        intermediate["insert_all"].append(insert_all(conn))
    print("Insert tests finished")

    for i in range(NUM_REPEATS):
        if i % 10 == 0:
            print(f"Querying INDEXED... {i} / {NUM_REPEATS}")
        intermediate["select_simple"].append(select_simple(conn))
        intermediate["select_sorting"].append(select_sorting(conn))
        intermediate["select_join"].append(select_join(conn))
        intermediate["select_group"].append(select_group(conn))
        intermediate["select_string"].append(select_string(conn, select_string_args))
        intermediate["select_aggregates"].append(select_aggregates(conn))

    print("Creating dummy table structure...")
    create_dummy(conn)
    for i in range(NUM_REPEATS):
        if i % 10 == 0:
            print(f"Querying DUMMY... {i} / {NUM_REPEATS}")
        intermediate["select_simple_dummy"].append(select_simple_dummy(conn))
        intermediate["select_join_dummy"].append(select_join_dummy(conn))
        intermediate["select_group_dummy"].append(select_group_dummy(conn))
        intermediate["select_string_dummy"].append(select_string_dummy(conn, select_string_args))
        intermediate["select_aggregates_dummy"].append(select_aggregates_dummy(conn))

    # print("Doubling data in \"employees\" and repeating queries...")
    # multiply_data_in_employees(conn)
    # for i in range(NUM_REPEATS):
    #     if i % 10 == 0:
    #         print(f"Querying MULTIPLIED... {i} / {NUM_REPEATS}")
    #     intermediate["select_simple_big"].append(select_simple(conn))
    #     intermediate["select_sorting_big"].append(select_sorting(conn))
    #     intermediate["select_join_big"].append(select_join(conn))
    #     intermediate["select_string_big"].append(select_string(conn, select_string_args))
    #     intermediate["select_aggregates_big"].append(select_aggregates(conn))
    #     intermediate["select_simple_big_dummy"].append(select_simple_dummy(conn))
    #     intermediate["select_join_big_dummy"].append(select_join_dummy(conn))
    #     intermediate["select_string_big_dummy"].append(select_string_dummy(conn, select_string_args))
    #     intermediate["select_aggregates_big_dummy"].append(select_aggregates_dummy(conn))

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

    return output, intermediate


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
    do_single_query(conn_mysql, queries.QUERY_DIS_FULL_GROUP_BY)
    log.i(">> ENGINE: MyISAM")
    do_single_query(conn_mysql, queries.QUERY_CHANGE_ENGINE_MYISAM)
    res_aggregated_my_isam, res_details_my_isam = do_tests_singlethread(conn_mysql, "mysql_isam")
    log.i(">> ENGINE: InnoDB")
    do_single_query(conn_mysql, queries.QUERY_CHANGE_ENGINE_INNODB)
    res_aggregated_my, res_details_my = do_tests_singlethread(conn_mysql, "mysql")

    log.i("> STEP 2: test MariaDB performance")
    conn_maria = MySQLdb.connect(host=MARIA_HOST,
                                 user=DB_USER,
                                 password=DB_PASS,
                                 db=DB_NAME,
                                 charset=DB_CSET)
    log.i(">> ENGINE: MyISAM")
    do_single_query(conn_maria, queries.QUERY_CHANGE_ENGINE_MYISAM)
    res_aggregated_ma_isam, res_details_ma_isam = do_tests_singlethread(conn_maria, "maria_isam")
    log.i(">> ENGINE: InnoDB")
    do_single_query(conn_maria, queries.QUERY_CHANGE_ENGINE_INNODB)
    res_aggregated_ma, res_details_ma = do_tests_singlethread(conn_maria, "maria")

    log.i("> STEP 3: write results")
    with open(f"logs/{NOW}-results.csv", "w") as output:
        writer = csv.DictWriter(output, OUTPUT_COLUMNS, dialect="excel")
        writer.writeheader()
        writer.writerows(res_aggregated_my)
        writer.writerows(res_aggregated_my_isam)
        writer.writerows(res_aggregated_ma)
        writer.writerows(res_aggregated_ma_isam)

    with open(f"logs/{NOW}-details.json", "w") as out_json:
        json.dump({
            "mysql": res_details_my,
            "maria": res_details_ma,
            "mysql_isam": res_details_my_isam,
            "maria_isam": res_details_ma_isam
        }, out_json)

    log.i(f">> saved to logs/{NOW}-results.csv")
    log.i(f">> saved details to logs/{NOW}-details.json")

    log.i("> STEP 4: Close database connections")
    conn_mysql.close()
    conn_maria.close()
    log.i("----- SCRIPT ENDED -----")
