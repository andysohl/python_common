# -*- coding: utf-8 -*-
import unittest

import src.analytics_common.email_support as email_support


class TestEmailNormaliseFunction(unittest.TestCase):
    def setUp(self):
        pass

    def test_whenNone_thenSuccess(self):
        res = email_support.normalise_email_addresses(None)
        self.assertIsNone(res)

    def test_whenEmpty_thenSuccess(self):
        res = email_support.normalise_email_addresses('')
        self.assertIsNone(res)

    def test_whenSingleUnwrappedEmail_thenSuccess(self):
        res = email_support.normalise_email_addresses('test1@example.com')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, 'test1@example.com')

    def test_whenSingleWrappedEmail_thenSuccess(self):
        res = email_support.normalise_email_addresses('"Test" <test1@example.com>')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, 'Test <test1@example.com>')

    def test_whenSingleWrappedWithCommaEmail_thenSuccess(self):
        res = email_support.normalise_email_addresses('"Test, Name" <test1@example.com>')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, '"Test, Name" <test1@example.com>')

    def test_whenSingleWrappedUnicodeWithCommaEmail_thenSuccess(self):
        res = email_support.normalise_email_addresses('"テスト, Name" <test1@example.com>')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, '=?utf-8?b?44OG44K544OILCBOYW1l?= <test1@example.com>')

    def test_whenMultipleUnwrappedEmail_thenSuccess(self):
        res = email_support.normalise_email_addresses('test1@example.com, test2@example.com')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 1)
        self.assertEqual(res, 'test1@example.com;test2@example.com')

    def test_whenMultipleUnorderedUnwrappedEmail_thenSuccess(self):
        res = email_support.normalise_email_addresses('test2@example.com, test1@example.com')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 1)
        self.assertEqual(res, 'test1@example.com;test2@example.com')

    def test_whenMultipleUnorderedWrappedEmail_thenSuccess(self):
        res = email_support.normalise_email_addresses('TestA <test2@example.com>, TestB <test1@example.com>')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 1)
        self.assertEqual(res, 'TestB <test1@example.com>;TestA <test2@example.com>')

    def test_whenMultipleWrappedWithCommaEmail_thenSuccess(self):
        res = email_support.normalise_email_addresses('"Test, A" <test1@example.com>, "Test, B" <test2@example.com>')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 1)
        self.assertEqual(res, '"Test, A" <test1@example.com>;"Test, B" <test2@example.com>')

    def test_whenSingleWrappedEmailWithDifferentName_thenSuccess(self):
        res = email_support.normalise_email_addresses('"Test, A" <test1@example.com>, "Test, B" <test1@example.com>')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, '"Test, A" <test1@example.com>')

    def test_whenSingleWrappedEmailWithOptionalName_thenSuccess(self):
        res = email_support.normalise_email_addresses('test1@example.com, "Test, B" <test1@example.com>')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, '"Test, B" <test1@example.com>')

    def test_whenInvalidEmail_thenFailure(self):
        res = email_support.normalise_email_addresses('-')
        self.assertIsNotNone(res)
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, '')

    def test_whenEqualLists_thenSuccess(self):
        res = email_support.compare_email_addresses('test1@example.com', 'test1@example.com')
        self.assertIsNotNone(res)
        self.assertEqual(res, True)

    def test_whenNonEqualLists_thenNotSuccess(self):
        res = email_support.compare_email_addresses('test1@example.com', 'test2@example.com')
        self.assertIsNotNone(res)
        self.assertEqual(res, False)

    def test_whenEqualWithDifferentWrapperLists_thenSuccess(self):
        res = email_support.compare_email_addresses('TestA <test1@example.com>', 'TestB <test1@example.com>')
        self.assertIsNotNone(res)
        self.assertEqual(res, True)


class TestEmailHashFunction(unittest.TestCase):
    def setUp(self):
        pass

    def test_whenNone_thenSuccess(self):
        res = email_support.hash_email_addresses(None)
        self.assertIsNone(res)

    def test_whenEmpty_thenSuccess(self):
        res = email_support.hash_email_addresses('')
        self.assertIsNone(res)

    def test_whenSingleUnwrappedEmail_thenSuccess(self):
        res = email_support.hash_email_addresses('test1@example.com')
        self.assertIsNotNone(res)
        self.assertEqual(res, 16154963039614707151)  # Predictable output.

    def test_whenDuplicateUnwrappedEmail_thenSuccess(self):
        res1 = email_support.hash_email_addresses('test1@example.com')
        self.assertIsNotNone(res1)

        res2 = email_support.hash_email_addresses('test1@example.com;test1@example.com')
        self.assertIsNotNone(res2)

        self.assertEqual(res2, res1)

    def test_whenWrappedAndUnwrappedEmail_thenSuccess(self):
        res1 = email_support.hash_email_addresses('test1@example.com')
        self.assertIsNotNone(res1)

        res2 = email_support.hash_email_addresses('Test <test1@example.com>')
        self.assertIsNotNone(res2)

        self.assertEqual(res2, res1)


class TestEmailFilterFunction(unittest.TestCase):
    def setUp(self):
        pass

    def test_whenNone_thenSuccess(self):
        res = email_support.filter_email_addresses(None, None)
        self.assertIsNone(res)

    def test_whenEmpty_thenSuccess(self):
        res = email_support.filter_email_addresses('', None)
        self.assertIsNone(res)

    def test_whenSingleUnwrappedEmailWithNoFilter_thenSuccess(self):
        res = email_support.filter_email_addresses('test1@example.com', None)
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, 'test1@example.com')

    def test_whenSingleUnwrappedEmailWithNoFilter_thenSuccess(self):
        res = email_support.filter_email_addresses('test1@example.com', None)
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, 'test1@example.com')

    def test_whenSingleUnwrappedEmailWithMatchingFilter_thenNone(self):
        res = email_support.filter_email_addresses('test1@example.com', ".*")
        self.assertIsNone(res)

    def test_whenTwoUnwrappedEmailWithMatchingFilter_thenOneRemaining(self):
        res = email_support.filter_email_addresses('test1@example.com;test2@example.com', "test1@example.com")
        self.assertEqual(res.count(";"), 0)  # only one email
        self.assertEqual(res, 'test2@example.com')

    def test_whenThreeUnwrappedEmailWithMatchingFilter_thenTwoRemaining(self):
        res = email_support.filter_email_addresses('test1@example.com;test2@example.com; Test3 <test3@example.com>', ".*2@example.com")
        self.assertEqual(res.count(";"), 1)
        self.assertEqual(res, 'test1@example.com;Test3 <test3@example.com>')


if __name__ == '__main__':
    unittest.main()
