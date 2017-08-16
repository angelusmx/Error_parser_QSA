# **********************************************************************************************
# Parser and interpreter of the log files of the Qiasymphony
#
# V1.2 - Integration of the individual modules under a single program control
# V1.1 - Extended subquery to find the failure cause beyond the previous line
# V1.0 -
# The previous row to the  message "F4 Grundstellung starten" contains in most cases the cause
# of the error (not in all cases true)
# the reason is then isolated and copied to the table "errors"
# the structure of the table is "ID", "Zeit Stamp", "error meldung"
# *********************************************************************************************

import os
from raw_data_parser import main_function
from error_parser import main_error_parser_M2
from error_parser_M1 import main_error_parser_M1
from Welding_data_parser import purify_welding_data

# Location of the log files to be processed

# ++++ Modul 2 ++++
logs_location_M2 = os.path.normpath('U:\\Tasks\\67_Data_analyzer_QSA\\logs_M2')
# ++++ Modul 1 ++++
logs_location_M1 = os.path.normpath('U:\\Tasks\\67_Data_analyzer_QSA\\logs_M1')

# Define the max number of months, must match the number of existing table in MySQL
# Max month is always one more as the count of table for the months in Mysql
max_month = 9

# Turn the welding data option on or off
process_welding = True

# Initialize the variable modul_number
modul_number = 2

# Execute the raw parsing of the Modul 2 Errors
if main_function(logs_location_M2, modul_number):

    # Call the error parser
    if main_error_parser_M2(max_month):

        print" \n\n --------- Parsing of Module 2 100% completed ---------- \n"

# Re-initialize the variable modul_number
modul_number = 1

# Execute the raw parsing of the Modul 1 Errors
if main_function(logs_location_M1, modul_number):

    # Call the error parser
    if main_error_parser_M1(max_month):

        print" \n\n --------- Parsing of Module 2 100% completed ---------- \n"

# Parse the welding data if set so by the variable
if process_welding:

    # Execute the welding parser (isolate and clean the weld data from the welding machine)
    if purify_welding_data(max_month):
        print "welding data parsed correctly"

else:

    print "No welding data will be parsed"













