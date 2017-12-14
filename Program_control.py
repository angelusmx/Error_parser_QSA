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
from raw_data_parser import main_function as raw_parser_main
from error_parser_M2 import main_error_parser_M2 as error_parser_m2
from error_parser_M1 import main_error_parser_M1 as error_parser_m1
from Zeit_messen import main as measure_time

# Location of the log files to be processed

# ++++ Modul 2 ++++
logs_location_M2 = os.path.normpath('U:\\Tasks\\67_Data_analyzer_QSA\\logs_M2')
# ++++ Modul 1 ++++
logs_location_M1 = os.path.normpath('U:\\Tasks\\67_Data_analyzer_QSA\\logs_M1')

# Turn the welding data option on or off
process_welding = True

# Initialize the variable modul_number
modul_number = 2

# Execute the parsing of the Modul 2 Errors from the files into the DB
# ***** This functions starts with the raw parsing of the data continuing with a direct parsing of the errors
# ***** The evaluation is performed per individual file and not cyclically over the tables anymore

measure_time()


if raw_parser_main(logs_location_M2, modul_number):
    print " ------- M2 Raw parsing complete -------- \n"

    # Execute the Error Analysis
    if error_parser_m2():
        print " -------- M2 Error parsing complete -------- \n"

# Re-initialize the variable modul_number
modul_number = 1

# Execute the raw parsing of the Modul 1 Errors
# ***** This functions starts with the raw parsing of the data continuing with a direct parsing of the errors
# ***** The evaluation is performed per individual file and not cyclically over the tables anymore
if raw_parser_main(logs_location_M1, modul_number):
    print " ------- M1 Raw parsing complete -------- \n"

    # Execute the Error Analysis
    if error_parser_m1():
        print " -------- M1 Error parsing complete -------- \n"













