import sys
import pathlib
# Add environment path to the SourceDataReader class programmatically (dynamic importing) 
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

    ### Methods for testing a single data row ###
    # OK data rows:  (unit_id, value, start, stop)
    row_01 = ("12345678901", "A", "2010-01-01", "2020-12-31")
    row_02 = ("12345678901", "A", "2010-01-01", "")
    row_03 = ("12345678901", "A", "2010-01-01", None)
    row_04 = ("AB_ASD_F113333", "12345", "1999-12-31", "2020-12-31")
    # Not valid data rows:
    row_10 = ("", "A", "2010-01-01", "2020-12-31")
    row_11 = (None, "A", "2010-01-01", "2020-12-31")
    row_12 = ("12345678901", "", "2010-01-01", "2020-12-31")
    row_13 = ("12345678901", None, "2010-01-01", "2020-12-31")
    row_14 = ("12345678901", "A", "", "2020-12-31")
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

    ### Methods for testing event-history (several data rows) ###
    # Event-history OK:
    rows_history_01 = [
        ("12345678901", "A", "2000-01-01", "2000-12-31"),
        ("12345678901", "B", "2001-01-01", "2001-12-31"),
        ("12345678901", "C", "2002-01-01", None)
    ]
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

    row_idx = 1
    prev_row_idx = 0
    # History OK
    def test_event_history_01(self):
        self.assertTrue(
            self.sdr.is_data_row_event_history_valid(
                self.rows_history_01[self.row_idx], 
                self.rows_history_01[self.prev_row_idx], 
                self.dummy_rownum
            )
        )

    # Not valid history
    def test_event_history_10(self):
        self.assertFalse(
            self.sdr.is_data_row_event_history_valid(
                self.rows_history_10[self.row_idx],
                self.rows_history_10[self.prev_row_idx],
                self.dummy_rownum
            )
        )

    def test_event_history_11(self):
        self.assertFalse(
            self.sdr.is_data_row_event_history_valid(
                self.rows_history_11[self.row_idx],
                self.rows_history_11[self.prev_row_idx],
                self.dummy_rownum
            )
        )

    def test_event_history_12(self):
        self.assertFalse(
            self.sdr.is_data_row_event_history_valid(
                self.rows_history_12[self.row_idx],
                self.rows_history_12[self.prev_row_idx],
                self.dummy_rownum
            )
        )

    def test_event_history_13(self):
        self.assertFalse(
            self.sdr.is_data_row_event_history_valid(
                self.rows_history_13[self.row_idx],
                self.rows_history_13[self.prev_row_idx],
                self.dummy_rownum
            )
        )


### MAIN ###
if __name__ == '__main__':
    unittest.main()
