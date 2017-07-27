import MySQLdb
import MySQLdb.cursors
from MySQLdb.cursors import DictCursor

# Create the connection to the DB
conn = MySQLdb.connect(host="localhost", user="root", passwd="Midvieditza12!", db="logs_qsa",
                       cursorclass=DictCursor)

# Create the cursor for the main query
cur_main = conn.cursor()
# Create the cursor for inserting the welding values
cur_weld = conn.cursor()


def test_connection():
    # Retrieves the version of the DB as evidence of the connection
    try:
        cur.execute("SELECT VERSION()")
        results = cur.fetchone()
        # Check if anything at all is returned
        if results:
            print("Database version : %s"), results
            return True
        else:
            print "ERROR IN CONNECTION"
            return False
    except MySQLdb.Error:
        return False

# *************** End of function **********************

def purify_welding_data(max_month)

    for i in range(1, max_month):

        # The name of the table to iterate through
        table = "m2_0" + str(i)

        print "Parsing errors in table " + table

        # The SQL Queries
        search_command = "SELECT * FROM " + table + " WHERE Meldung LIKE %s"
        search_string = '%' + 'Anweisung: Strung beseitigen' + '%'

