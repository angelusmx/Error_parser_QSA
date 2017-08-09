# **********************************************************************************************
# Using the Levenshtein algorithm to determine the distance between the current error and
# the generalized errors (including step number) order the error to one of the generalized ones
# the selected information will be then input into the table "errors"
# *********************************************************************************************

import Levenshtein as Lev
import MySQLdb.cursors
from MySQLdb.cursors import DictCursor

# define the containers for the string
string_a = ""
string_b = ""


# Create the connection to the database
conn = MySQLdb.connect(host="HIL-ENG-L1", user="a.canizales", passwd="1123581321345589", db="logs_qsa",
                       cursorclass=DictCursor)
# Create the cursor for the main query
cur = conn.cursor()
# Create the cursor for the table query
cur_table = conn.cursor()


def count_rows():
    # Count the number of current rows in der table "generalized error"!
    sql_command = "SELECT COUNT(*) FROM generalized_errors"
    cur.execute(sql_command)

    # Pull the result from the cursor into the variable
    row_count = cur.fetchone()

    return row_count


def match_strings(sample_string):

    # Variable that contains the current best Levensthein match
    best_distance = 300

    # Variable that contains the best text approximation based on "best_distance"
    best_match_error = ""

    # Fetch the errors list from the Database
    sql_retrieve = "SELECT general_error FROM generalized_errors"
    cur.execute(sql_retrieve)

    # Fetch one row to start the process
    row_error = cur.fetchone()

    # Iterate through the list
    while row_error is not None:

        dist = Lev.distance(sample_string, row_error['general_error'])

        # Compare the distance against the current "champion"
        if dist < best_distance:
            best_distance = dist
            best_match_error = row_error['general_error']

        # Pull the next value from the table
        row_error = cur.fetchone()

    if best_distance <= 10:
        return best_match_error
    else:
        best_match_error = "No match found"
        return best_match_error











