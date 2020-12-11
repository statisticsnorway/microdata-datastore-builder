import sys
import pathlib
# Add environment path to the SourceDataReader class programmatically (dynamic import) 
# to avoid "ValueError: attempted relative import beyond top-level package".
# See https://stackoverflow.com/questions/11393492/python-package-import-from-parent-directory
# Example: sys.path.insert(1, "/home/myprojects/GitHub/statisticsnorway/microdata-datastore-builder/validate")
path_project_root = pathlib.Path(__file__).parent.parent.parent
path_validate = str(path_project_root) + "/validate"
sys.path.insert(1, path_validate)
#print(sys.path)

"""
Unit tests for the SourceDataReader class.
"""

import SourceDataReader
import unittest

class TestSourceDataReader(unittest.TestCase):

    sdr = SourceDataReader.SourceDataReader("dummy_file")
    dummy_rownum = 1


    ### TODO methods for
    # create test csv file (semikolon separert string til fil)
    # read test csv file into db 
    # validate db-data
    # clean up db and test data


    #############################################
    ### Methods for testing a single data row ###
    ############################################# 

    # OK data rows:  (unit_id, value, start, stop)
    row_01 = ("12345678901", "A", "2010-01-01", "2020-12-31")
    row_02 = ("12345678901", "A", "2010-01-01", "")
    row_03 = ("12345678901", "A", "2010-01-01", None)
    row_04 = ("AB_ASD_F113333", "12345", "1999-12-31", "2020-12-31")
    row_05 = ("AB_ASD_F113333", "12345", None, None)  # OK for FIXED dataset
    row_06 = ("AB_ASD_F113333", "12345", "", "")  # OK for FIXED dataset

    # Not valid data rows:
    row_10 = ("", "A", "2010-01-01", "2020-12-31")
    row_11 = (None, "A", "2010-01-01", "2020-12-31")
    row_12 = ("12345678901", "", "2010-01-01", "2020-12-31")
    row_13 = ("12345678901", None, "2010-01-01", "2020-12-31")
    row_14 = ("12345678901", "A", "", "2020-12-31") # Stop-date exists, but missing start-date
    row_15 = ("12345678901", "A", "2020-88-99", None) # Not valid start-date
    row_16 = ("12345678901", "", "2010-01-01", "2020-88-99") # Not valid stop-date
    row_17 = ("12345678901", "", "2020-01-01", "1999-01-01") # Start-date greater than stop-date
    row_18 = (None)
    row_19 = ("")
    row_20 = ("12345678901", None)
    row_21 = ("12345678901", "A", None)
    row_22 = ("12345678901", "A", "2020-01-01")

    # OK data rows
    def test_row_01(self):
        self.assertTrue(
            self.sdr.is_data_row_valid(self.row_01, self.dummy_rownum)
        )

    def test_row_02(self):
        self.assertTrue(
            self.sdr.is_data_row_valid(self.row_02, self.dummy_rownum)
        )

    def test_row_03(self):
        self.assertTrue(
            self.sdr.is_data_row_valid(self.row_03, self.dummy_rownum)
        )

    def test_row_04(self):
        self.assertTrue(
            self.sdr.is_data_row_valid(self.row_04, self.dummy_rownum)
        )

    def test_row_05(self):
        self.assertTrue(
            self.sdr.is_data_row_valid(self.row_05, self.dummy_rownum)
        )

    def test_row_06(self):
        self.assertTrue(
            self.sdr.is_data_row_valid(self.row_06, self.dummy_rownum)
        )

    # Not valid data rows:
    def test_row_10(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_10, self.dummy_rownum)
        )

    def test_row_11(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_11, self.dummy_rownum)
        )

    def test_row_12(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_12, self.dummy_rownum)
        )

    def test_row_13(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_13, self.dummy_rownum)
        )

    def test_row_14(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_14, self.dummy_rownum)
        )

    def test_row_15(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_15, self.dummy_rownum)
        )

    def test_row_16(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_16, self.dummy_rownum)
        )

    def test_row_17(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_17, self.dummy_rownum)
        )

    def test_row_18(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_18, self.dummy_rownum)
        )

    def test_row_19(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_19, self.dummy_rownum)
        )

    def test_row_20(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_20, self.dummy_rownum)
        )

    def test_row_21(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_21, self.dummy_rownum)
        )

    def test_row_22(self):
        self.assertFalse(
            self.sdr.is_data_row_valid(self.row_22, self.dummy_rownum)
        )


    ##############################
    ### Consistency validation ###
    ############################## 

    ### Methods for temporalityType = FIXED  ###
    # OK data
    rows_fixed_01 = [
        ("12345678901", "AAAAAAAAA", "", ""),
        ("99999999999", "BBBBBBBBB", "", "")
    ]

    def test_fixed_01(self):
        self.sdr.set_meta_temporality_type("FIXED")
        self.assertTrue(
            self.sdr.is_data_row_consistent(
                self.rows_fixed_01[0], 
                self.rows_fixed_01[1], 
                self.dummy_rownum
            )
        )

    # Not valid
    rows_fixed_10 = [
        ("12345678901", "AAAAAAAAA", "", ""),
        ("12345678901", "BBBBBBBBB", "", "")  # same unit_id
    ]

    rows_fixed_11 = [
        ("12345678901", "AAAAAAAAA", "2020-01-01", ""),   # Start-date is not null (no date for fixed dateset)
        ("99999999999", "BBBBBBBBB", "", "")  
    ]

    rows_fixed_12 = [
        ("12345678901", "AAAAAAAAA", "", "2020-12-31"),   # Stop-date is not null (no date for fixed dateset)
        ("99999999999", "BBBBBBBBB", "", "")  
    ]

    def test_fixed_10(self):
        self.sdr.set_meta_temporality_type("FIXED")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_fixed_10[0], 
                self.rows_fixed_10[1], 
                self.dummy_rownum
            )
        )

    def test_fixed_11(self):
        self.sdr.set_meta_temporality_type("FIXED")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_fixed_11[0], 
                self.rows_fixed_11[1], 
                self.dummy_rownum
            )
        )

    def test_fixed_12(self):
        self.sdr.set_meta_temporality_type("FIXED")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_fixed_12[0], 
                self.rows_fixed_12[1], 
                self.dummy_rownum
            )
        )


    ### Methods for temporalityType = STATUS  ###
    # OK data
    rows_status_01 = [
        ("12345678901", "AAAAAAAAA", "2020-12-31", "2020-12-31"),
        ("99999999999", "BBBBBBBBB", "2020-12-31", "2020-12-31")
    ]

    def test_status_01(self):
        self.sdr.set_meta_temporality_type("STATUS")
        self.assertTrue(
            self.sdr.is_data_row_consistent(
                self.rows_status_01[0], 
                self.rows_status_01[1], 
                self.dummy_rownum
            )
        )

    # Not valid
    rows_status_10 = [
        ("12345678901", "AAAAAAAAA", "2020-12-31", "2020-12-31"),
        ("12345678901", "BBBBBBBBB", "2020-12-31", "2020-12-31")  # not valid - same unit_id and start-date ("duplicate")
    ]

    rows_status_11 = [
        ("12345678901", "AAAAAAAAA", "", "2020-12-31"),  # Start-date missing.
        ("12345678901", "BBBBBBBBB", "", "2020-12-31")
    ]

    rows_status_12 = [
        ("12345678901", "AAAAAAAAA", "2020-12-31", ""),  # Stop-date missing.
        ("12345678901", "BBBBBBBBB", "2020-12-31", "")
    ]

    rows_status_13 = [
        ("12345678901", "AAAAAAAAA", "2020-01.01", "2020-12.31"),  # Start-date and Stop-date not equal.
        ("12345678901", "BBBBBBBBB", "2020-01.01", "2020-12.31")
    ]

    def test_status_10(self):
        self.sdr.set_meta_temporality_type("STATUS")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_status_10[0], 
                self.rows_status_10[1], 
                self.dummy_rownum
            )
        )

    def test_status_11(self):
        self.sdr.set_meta_temporality_type("STATUS")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_status_11[0], 
                self.rows_status_11[1], 
                self.dummy_rownum
            )
        )

    def test_status_12(self):
        self.sdr.set_meta_temporality_type("STATUS")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_status_12[0], 
                self.rows_status_12[1], 
                self.dummy_rownum
            )
        )

    def test_status_13(self):
        self.sdr.set_meta_temporality_type("STATUS")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_status_13[0], 
                self.rows_status_13[1], 
                self.dummy_rownum
            )
        )



    ### Methods for testing event-history (consistency in several data rows as a set) ###
    ### Methods for temporalityType = EVENT ###
    row_idx = 1
    prev_row_idx = 0

    # Event-history OK:
    rows_history_01 = [
        ("12345678901", "A", "2000-01-01", "2000-12-31"),
        ("12345678901", "B", "2001-01-01", "2001-12-31"),
        ("12345678901", "C", "2002-01-01", None)
    ]

    # History OK
    def test_event_history_01(self):
        self.sdr.set_meta_temporality_type("EVENT")
        self.assertTrue(
            self.sdr.is_data_row_consistent(
                self.rows_history_01[self.row_idx], 
                self.rows_history_01[self.prev_row_idx], 
                self.dummy_rownum
            )
        )


    # Event-history not valid:
    rows_history_10 = [
        ("12345678901", "A", "2000-01-01", "2000-12-31"),
        ("12345678901", "B", "2000-01-01", None)  # Not valid - same unit_id and start-date as previous row
    ]
    rows_history_11 = [
        ("12345678901", "A", "2000-01-01", None),
        ("12345678901", "B", "2000-01-01", None)  # Not valid - same unit_id and start-date as previous row
    ]
    rows_history_12 = [
        ("12345678901", "A", "2000-01-01", "2000-12-31"),
        ("12345678901", "A", "2000-01-01", "2000-12-31")  # Duplicate - same as previous row
    ]
    rows_history_13 = [
        ("12345678901", "A", "2000-01-01", "2003-12-31"),
        ("12345678901", "B", "2001-01-01", None)  # Not valid - previous stop-date is greater than start
    ]
    rows_history_14 = [
        ("12345678901", "A", "2000-01-01", None),
        ("12345678901", "B", "2001-01-01", "2001-12-31") # Not valid - previous row not ended (missing stop)
    ]
    rows_history_15 = [
        ("12345678901", "A", "2000-01-01", ""),
        ("12345678901", "B", "2001-01-01", "2001-12-31") # Not valid - previous row not ended (missing stop)
    ]

    # Not valid history
    def test_event_history_10(self):
        self.sdr.set_meta_temporality_type("EVENT")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_history_10[self.row_idx],
                self.rows_history_10[self.prev_row_idx],
                self.dummy_rownum
            )
        )

    def test_event_history_11(self):
        self.sdr.set_meta_temporality_type("EVENT")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_history_11[self.row_idx],
                self.rows_history_11[self.prev_row_idx],
                self.dummy_rownum
            )
        )

    def test_event_history_12(self):
        self.sdr.set_meta_temporality_type("EVENT")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_history_12[self.row_idx],
                self.rows_history_12[self.prev_row_idx],
                self.dummy_rownum
            )
        )

    def test_event_history_13(self):
        self.sdr.set_meta_temporality_type("EVENT")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_history_13[self.row_idx],
                self.rows_history_13[self.prev_row_idx],
                self.dummy_rownum
            )
        )

    def test_event_history_14(self):
        self.sdr.set_meta_temporality_type("EVENT")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_history_14[self.row_idx],
                self.rows_history_14[self.prev_row_idx],
                self.dummy_rownum
            )
        )

    def test_event_history_15(self):
        self.sdr.set_meta_temporality_type("EVENT")
        self.assertFalse(
            self.sdr.is_data_row_consistent(
                self.rows_history_15[self.row_idx],
                self.rows_history_15[self.prev_row_idx],
                self.dummy_rownum
            )
        )

### MAIN ###
if __name__ == '__main__':
    unittest.main()
