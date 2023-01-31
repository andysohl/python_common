# -*- coding: utf-8 -*-
import logging
import logging.config
import unittest

from src.analytics_common.msgraph import MsGraphClient
from src.analytics_common.excel_support import open_excel


class TestMsGraphClient(unittest.TestCase):
    __logger: logging.Logger

    def setUp(self):
        logging.config.fileConfig("test/logging.conf")
        self.__logger = logging.getLogger(self.__class__.__name__)

    def test_whenNone_thenSuccess(self):
        pass
        msgc = MsGraphClient(False, debug=True)
        self.assertIsNotNone(msgc)

        msgc.authenticate("client_id", "client_secret", "tenant")

        location = "https://tricor365.sharepoint.com/sites/SingaporeClientService/Operations/00. Operational/01-Billing Configuration.xlsx"
        # Download a file
        data = msgc.download_sharepoint_file(location)

        # Download the file again to make sure the cache is working
        data = msgc.download_sharepoint_file(location)

        # Open as excel
        wb = open_excel(data)

        # Read into a dataframe


if __name__ == '__main__':
    unittest.main()
