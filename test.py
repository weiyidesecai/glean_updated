import datetime
from decimal import Decimal
import unittest
import invoice
from pyspark.sql.types import Row

class InvoiceTest(unittest.TestCase):
    def test_vendor_not_seen_in_a_while(self):
        res = invoice.map_vendor_not_seen_in_a_while((
            'test_vendor_id',
            [
                Row(invoice_id='invoice_1', invoice_date=datetime.date(2020, 1, 1), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='invoice_2', invoice_date=datetime.date(2020, 4, 1), canonical_vendor_id='test_vendor_id'),
            ]
        ))
        self.assertEqual(next(res), (
            datetime.date(2020, 4, 1),
            "First new bill in 3 months from vendor test_vendor_id",
            'vendor_not_seen_in_a_while',
            'invoice',
            'invoice_2',
            'test_vendor_id',
        ))
        with self.assertRaises(StopIteration):
            next(res)

    def test_vendor_not_seen_in_a_while_not_triggered(self):
        res = invoice.map_vendor_not_seen_in_a_while((
            'test_vendor_id',
            [
                Row(invoice_id='invoice_1', invoice_date=datetime.date(2020, 1, 1), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='invoice_2', invoice_date=datetime.date(2020, 2, 1), canonical_vendor_id='test_vendor_id'),
            ]
        ))
        with self.assertRaises(StopIteration):
            next(res)

    def test_map_accrual_alert__invoice(self):
        res = invoice.map_accrual_alert((
            'test_invoice',
            (
                [
                    Row(line_item_id='item_1', period_end_date=datetime.date(2020, 1, 2)),
                ],
                Row(invoice_id='test_invoice', invoice_date=datetime.date(2020, 1, 1), period_end_date=datetime.date(2020, 4, 1), canonical_vendor_id='test_vendor_id'),
            )
        ))
        self.assertEqual(next(res), (
            datetime.date(2020, 1, 1),
            "Line items from vendor test_vendor_id in this invoice cover future periods (through 2020-04-01)",
            'accrual_alert',
            'invoice',
            'test_invoice',
            'test_vendor_id',
        ))
        with self.assertRaises(StopIteration):
            next(res)

    def test_map_accrual_alert__item(self):
        res = invoice.map_accrual_alert((
            'test_invoice',
            (
                [
                    Row(line_item_id='item_1', period_end_date=datetime.date(2020, 4, 2)),
                ],
                Row(invoice_id='test_invoice', invoice_date=datetime.date(2020, 1, 1), period_end_date=datetime.date(2020, 2, 1), canonical_vendor_id='test_vendor_id'),
            )
        ))
        self.assertEqual(next(res), (
            datetime.date(2020, 1, 1),
            "Line items from vendor test_vendor_id in this invoice cover future periods (through 2020-04-02)",
            'accrual_alert',
            'invoice',
            'test_invoice',
            'test_vendor_id',
        ))
        with self.assertRaises(StopIteration):
            next(res)

    def test_map_accrual_alert__no_end(self):
        res = invoice.map_accrual_alert((
            'test_invoice',
            (
                [
                    Row(line_item_id='item_1', period_end_date=None),
                ],
                Row(invoice_id='test_invoice', invoice_date=datetime.date(2020, 1, 1), period_end_date=None, canonical_vendor_id='test_vendor_id'),
            )
        ))
        with self.assertRaises(StopIteration):
            next(res)

    def test_map_large_month_increase_mtd__first(self):
        res = invoice.map_large_month_increase_mtd((
            'test_vendor_id',
            [
                Row(invoice_id='test_invoice', invoice_date=datetime.date(2020, 1, 1), total_amount=Decimal(1), canonical_vendor_id='test_vendor_id'),
            ]
        ))
        with self.assertRaises(StopIteration):
            next(res)

    def test_map_large_month_increase_mtd__trigger(self):
        data = [
            (200, 2000),
            (2000, 7000),
            (20000, 35000),
        ]
        for avg, cur in data:
            with self.subTest(avg=avg, cur=cur):
                res = invoice.map_large_month_increase_mtd((
                    'test_vendor_id',
                    [
                        Row(invoice_id='test_unused_invoice', invoice_date=datetime.date(2019, 12, 3), total_amount=Decimal(123456789), canonical_vendor_id='test_vendor_id'),
                        Row(invoice_id='test_avg_invoice', invoice_date=datetime.date(2020, 5, 1), total_amount=Decimal(avg*12), canonical_vendor_id='test_vendor_id'),
                        Row(invoice_id='test_invoice', invoice_date=datetime.date(2021, 1, 1), total_amount=Decimal(cur), canonical_vendor_id='test_vendor_id'),
                    ]
                ))
                self.assertEqual(next(res), (
                    datetime.date(2021, 1, 1),
                    f"Monthly spend with test_vendor_id is {cur-avg:.2f} ({(cur-avg)/avg:.0%}) higher than average",
                    'large_month_increase_mtd',
                    'vendor',
                    'test_invoice',
                    'test_vendor_id',
                ))
                with self.assertRaises(StopIteration):
                    next(res)

    def test_map_large_month_increase_mtd__not_triggered(self):
        data = [
            (200, 1000),
            (2000, 5000),
            (20000, 15000),
        ]
        for avg, cur in data:
            with self.subTest(avg=avg, cur=cur):
                res = invoice.map_large_month_increase_mtd((
                    'test_vendor_id',
                    [
                        Row(invoice_id='test_unused_invoice', invoice_date=datetime.date(2019, 12, 3), total_amount=Decimal(123456789), canonical_vendor_id='test_vendor_id'),
                        Row(invoice_id='test_avg_invoice', invoice_date=datetime.date(2020, 5, 1), total_amount=Decimal(avg*12), canonical_vendor_id='test_vendor_id'),
                        Row(invoice_id='test_invoice', invoice_date=datetime.date(2021, 1, 1), total_amount=Decimal(cur), canonical_vendor_id='test_vendor_id'),
                    ]
                ))
                with self.assertRaises(StopIteration):
                    next(res)

    def test_map_no_invoice_received__monthly__end_of_month(self):
        res = invoice.map_no_invoice_received((
            'test_vendor_id',
            [
                Row(invoice_id='test_invoice_1', invoice_date=datetime.date(2020, 1, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_2', invoice_date=datetime.date(2020, 2, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_3', invoice_date=datetime.date(2020, 3, 25), canonical_vendor_id='test_vendor_id'),
            ]
        ))
        self.assertEqual(list(res), [
            (
                datetime.date(2020, 4, i),
                f"test_vendor_id generally charges between on 25 day of each month invoices are sent. On 2020-04-25, an invoice from test_vendor_id has not been received",
                'no_invoice_received',
                'vendor',
                None,
                'test_vendor_id',
            ) for i in range(25, 31)
        ])

    def test_map_no_invoice_received__monthly__recieved(self):
        res = invoice.map_no_invoice_received((
            'test_vendor_id',
            [
                Row(invoice_id='test_invoice_1', invoice_date=datetime.date(2020, 1, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_2', invoice_date=datetime.date(2020, 2, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_3', invoice_date=datetime.date(2020, 3, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_3', invoice_date=datetime.date(2020, 4, 27), canonical_vendor_id='test_vendor_id'),
            ]
        ))
        self.assertEqual([r for r in res if r[0].month == 4], [
            (
                datetime.date(2020, 4, i),
                f"test_vendor_id generally charges between on 25 day of each month invoices are sent. On 2020-04-25, an invoice from test_vendor_id has not been received",
                'no_invoice_received',
                'vendor',
                None,
                'test_vendor_id',
            ) for i in range(25, 27)
        ])

    def test_map_no_invoice_received__quarterly__end_of_month(self):
        res = invoice.map_no_invoice_received((
            'test_vendor_id',
            [
                Row(invoice_id='test_invoice_1', invoice_date=datetime.date(2020, 1, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_2', invoice_date=datetime.date(2020, 4, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_3', invoice_date=datetime.date(2020, 7, 25), canonical_vendor_id='test_vendor_id'),
            ]
        ))
        self.assertEqual(list(res), [
            (
                datetime.date(2020, 10, i),
                f"test_vendor_id generally charges between on 25 day of each month invoices are sent. On 2020-10-25, an invoice from test_vendor_id has not been received",
                'no_invoice_received',
                'vendor',
                None,
                'test_vendor_id',
            ) for i in range(25, 32)
        ])

    def test_map_no_invoice_received__quarterly__recieved(self):
        res = invoice.map_no_invoice_received((
            'test_vendor_id',
            [
                Row(invoice_id='test_invoice_1', invoice_date=datetime.date(2020, 1, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_2', invoice_date=datetime.date(2020, 4, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_3', invoice_date=datetime.date(2020, 7, 25), canonical_vendor_id='test_vendor_id'),
                Row(invoice_id='test_invoice_3', invoice_date=datetime.date(2020, 10, 27), canonical_vendor_id='test_vendor_id'),
            ]
        ))
        self.assertEqual([r for r in res if r[0].month == 10], [
            (
                datetime.date(2020, 10, i),
                f"test_vendor_id generally charges between on 25 day of each month invoices are sent. On 2020-10-25, an invoice from test_vendor_id has not been received",
                'no_invoice_received',
                'vendor',
                None,
                'test_vendor_id',
            ) for i in range(25, 27)
        ])
