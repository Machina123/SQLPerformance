TABLES = ["departments", "dept_manager", "dept_emp", "titles", "salaries"]

QUERYLIST_CREATE_STRUCTURE = [
    "DROP TABLE IF EXISTS dept_emp, dept_manager, titles, salaries, employees, departments;",
    """CREATE TABLE employees (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      ENUM ('M','F')  NOT NULL,    
    hire_date   DATE            NOT NULL,
    PRIMARY KEY (emp_no)
    );""",
    """CREATE TABLE departments (
    dept_no     CHAR(4)         NOT NULL,
    dept_name   VARCHAR(40)     NOT NULL,
    PRIMARY KEY (dept_no),
    UNIQUE  KEY (dept_name)
    );""",
    """CREATE TABLE dept_manager (
   emp_no       INT             NOT NULL,
   dept_no      CHAR(4)         NOT NULL,
   from_date    DATE            NOT NULL,
   to_date      DATE            NOT NULL,
   FOREIGN KEY (emp_no)  REFERENCES employees (emp_no)    ON DELETE CASCADE,
   FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
   PRIMARY KEY (emp_no,dept_no)
   );""",
    """CREATE TABLE dept_emp (
    emp_no      INT             NOT NULL,
    dept_no     CHAR(4)         NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE            NOT NULL,
    FOREIGN KEY (emp_no)  REFERENCES employees   (emp_no)  ON DELETE CASCADE,
    FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no,dept_no)
    );""",
    """CREATE TABLE titles (
    emp_no      INT             NOT NULL,
    title       VARCHAR(50)     NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE,
    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no,title, from_date)
    );""",
    """CREATE TABLE salaries (
    emp_no      INT             NOT NULL,
    salary      INT             NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE            NOT NULL,
    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no, from_date)
    );"""
]

QUERY_DROP_STRUCTURE = "DROP TABLE IF EXISTS dept_emp, dept_manager, titles, salaries, departments;"

QUERYLIST_CREATE_DUMMY_STRUCTURE = [
    "CREATE TABLE dummy_departments SELECT * FROM departments;",
    "CREATE TABLE dummy_dept_manager SELECT * FROM dept_manager;",
    "CREATE TABLE dummy_dept_emp SELECT * FROM dept_emp;",
    "CREATE TABLE dummy_titles SELECT * FROM titles;",
    "CREATE TABLE dummy_salaries SELECT * FROM salaries;"
]

QUERY_SELECT_SIMPLE = """SELECT * FROM `employees` 
JOIN `titles` ON `titles`.`emp_no` = `employees`.`emp_no`
WHERE `gender`='F' AND `title`='Engineer';
"""

QUERY_SELECT_SORT = "SELECT * FROM employees ORDER BY `last_name`;"

QUERY_SELECT_JOIN = """SELECT `first_name`, `last_name`, `title` FROM `employees`
JOIN `titles` ON `employees`.`emp_no` = `titles`.`emp_no`
WHERE `from_date` < '2000-01-01' AND `to_date` > '2000-01-01';
"""

QUERY_SELECT_GROUP = """SELECT `departments`.`dept_name`, COUNT(1) FROM `dept_emp`
JOIN `departments` ON `departments`.`dept_no` = `dept_emp`.`dept_no`
GROUP BY `departments`.`dept_no`;
"""

QUERY_SELECT_AGGREGATE = """SELECT `employees`.`first_name`, `employees`.`last_name`, 
SUM(`salaries`.`salary`) AS SumSalary,
AVG(`salaries`.`salary`) AS AvgSalary, 
MAX(`salaries`.`salary`) AS MaxSalary, 
MIN(`salaries`.`salary`) AS MinSalary
FROM `salaries`
JOIN `employees` ON `employees`.`emp_no` = `salaries`.`emp_no`
GROUP BY `employees`.`emp_no`
ORDER BY `AvgSalary` DESC; 
"""


QUERY_DIS_FULL_GROUP_BY = "SET SESSION sql_mode = TRIM(BOTH ',' FROM REPLACE(@@SESSION.sql_mode, 'ONLY_FULL_GROUP_BY', ''));"

QUERY_CHANGE_ENGINE_MYISAM = "SET default_storage_engine='MyISAM';"
QUERY_CHANGE_ENGINE_INNODB = "SET default_storage_engine='InnoDB';"

QUERY_MULTIPLY_EMPLOYEES = "INSERT INTO `employees` SELECT * FROM `employees`;"


def query_select_string(args):
    return f"""SELECT `first_name`, `last_name` FROM `employees` 
    WHERE `first_name` LIKE '{args[0]}%' AND `last_name` LIKE '{args[1]}%';
    """


def make_query_on_dummy(query: str):
    for table in TABLES:
        query = query.replace(table, f"dummy_{table}")
    return query
