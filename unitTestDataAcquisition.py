import unittest
from dataQuality import *
from inputTest import *

#ordinamento
unittest.TestLoader.sortTestMethodsUsing = lambda *args: -1

#overriding costruttore
class DataQuality(DataQuality):
    def __init__(self):
        pass


class TestDataQuality(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
       cls.dq=DataQuality()

    def test_standardize_date_UNT_FVM_PIC_02(self):
        ret=self.dq.standardizeDate(date_UNT_FVM_PIC_02)
        self.assertEqual(ret,ret)

    def test_standardize_date_UNT_FVM_PIC_03(self):
        for date in date_UNT_FVM_PIC_03:
            ret = self.dq.standardizeDate(date)
            try:
                datetime.strptime(ret, "%Y-%m-%dT%H:%M:%SZ")
                self.assertTrue(True)
            except:
                self.assertTrue(False)

    def test_standardize_date_UNT_FVM_PIC_04(self):
        for date in date_UNT_FVM_PIC_04:
            ret = self.dq.standardizeDate(date)
            try:
                datetime.strptime(ret, "%Y-%m-%dT%H:%M:%SZ")
                self.assertTrue(True)
            except:
                self.assertTrue(False)

    def test_standardize_date_UNT_FVM_PIC_05(self):
        for date in date_UNT_FVM_PIC_05:
            ret = self.dq.standardizeDate(date)
            self.assertIsNone(ret)

    def test_capitalize_first_letter_UNT_FVM_PIC_06(self):
        for strT in str_UNT_FVM_PIC_06:
            ret = self.dq.capitalizeFirstLetter(strT)
            self.assertTrue(ret[0].isupper())