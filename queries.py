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

QUERY_SELECT_SIMPLE = "SELECT * FROM employees WHERE `gender`='F';"

QUERY_SELECT_SORT = "SELECT * FROM employees ORDER BY `last_name` LIMIT 10000;"

QUERY_SELECT_JOIN = """SELECT `first_name`, `last_name`, `title` FROM `employees`
JOIN `titles` ON `employees`.`emp_no` = `titles`.`emp_no`
WHERE `from_date` > '2000-01-01' AND `to_date` < '2000-01-01'
LIMIT 10000;
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


def query_select_string(args):
    return f"""SELECT `first_name`, `last_name` FROM `employees` 
    WHERE `first_name` LIKE '{args[0]}%' AND `last_name` LIKE '{args[1]}%';
    """
