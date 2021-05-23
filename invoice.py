import datetime
from decimal import Decimal
import uuid

from typing import Counter, Iterable, List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, Row, StringType, StructField, StructType


invoice_schema = StructType([
    StructField("invoice_id", StringType(), False),
    StructField("invoice_date", DateType(), False),
    StructField("due_date", DateType(), False),
    StructField("period_start_date", DateType(), False),
    StructField("period_end_date", DateType(), False),
    StructField("total_amount", DecimalType(scale=2), False),
    StructField("canonical_vendor_id", StringType(), False),
])
line_item_schema = StructType([
    StructField("invoice_id", StringType(), False),
    StructField("line_item_id", StringType(), False),
    StructField("period_start_date", DateType(), False),
    StructField("period_end_date", DateType(), False),
    StructField("total_amount", DecimalType(scale=2), False),
    StructField("canonical_line_item_id", StringType(), False),
])

def map_vendor_not_seen_in_a_while(p: Tuple[str, Iterable[Row]]):
    vendor_id, ins = p
    ins = sorted(ins, key=lambda i: i.invoice_date)
    for i, invoice in enumerate(ins):
        if i == 0:
            continue
        time: datetime.date = invoice.invoice_date
        previous_time: datetime.date = ins[i - 1].invoice_date
        delta = time - previous_time
        if delta > datetime.timedelta(days=90):
            yield (
                invoice.invoice_date,
                f"First new bill in {delta.days // 30} months from vendor {vendor_id}",
                'vendor_not_seen_in_a_while',
                'invoice',
                invoice.invoice_id,
                vendor_id,
            )

def map_accrual_alert(p: Tuple[str, Tuple[Iterable[Row], Row]]):
    invoice_id, (items, invoice) = p

    def ends():
        for i in items:
            if i.period_end_date is not None:
                yield i.period_end_date
        if invoice.period_end_date is not None:
            yield invoice.period_end_date

    max_end = max(ends(), default=None)
    if max_end is not None and max_end - invoice.invoice_date > datetime.timedelta(days=90):
        yield (
            invoice.invoice_date,
            f"Line items from vendor {invoice.canonical_vendor_id} in this invoice cover future periods (through {max_end})",
            'accrual_alert',
            'invoice',
            invoice_id,
            invoice.canonical_vendor_id,
        )

def map_large_month_increase_mtd(p: Tuple[str, Iterable[Row]]):
    vendor_id, ins = p
    ins = sorted(ins, key=lambda i: i.invoice_date)
    current_month = datetime.date.min
    current_month_spend = Decimal(0)
    history_spend: List[Tuple[datetime.date, Decimal]] = []
    history_totol_spend = Decimal(0)
    for i in ins:
        month: datetime.date = i.invoice_date.replace(day=1)
        if month > current_month:
            history_spend.append((current_month, current_month_spend))
            history_totol_spend += current_month_spend
            current_month = month
            current_month_spend = Decimal(0)
        if history_spend:
            oldest_month, oldest_month_spend = history_spend[0]
            if month.replace(year=month.year - 1) > oldest_month:
                history_spend.pop(0)
                history_totol_spend -= oldest_month_spend

        current_month_spend += i.total_amount
        avg = float(history_totol_spend) / 12
        if avg == 0.:
            triggered = False
        elif current_month_spend > 10_000:
            triggered = current_month_spend > avg * 1.5
        elif current_month_spend > 1_000:
            triggered = current_month_spend > avg * 3.0
        elif current_month_spend > 100:
            triggered = current_month_spend > avg * 6.0
        else:
            triggered = False

        if triggered:
            inc = float(current_month_spend) - avg
            rate = inc / avg
            yield (
                i.invoice_date,
                f"Monthly spend with {vendor_id} is {inc:.2f} ({rate:.0%}) higher than average",
                'large_month_increase_mtd',
                'vendor',
                i.invoice_id,
                vendor_id,
            )

def usual_key(counter: Counter):
    k0, v0 = counter.most_common()[0]
    for k, v in counter.most_common():
        if v < v0:
            return k0
        k0 = min(k, k0)
    return k0

def current_quarter(date: datetime.date):
    return date.replace(day=1, month=(date.month - 1) // 3 * 3 + 1)

def map_no_invoice_received(p: Tuple[str, Iterable[Row]]):
    vendor_id, ins = p
    ins = sorted(ins, key=lambda i: i.invoice_date)

    monthly_history = []
    def warn_monthly(next_date: datetime.date):
        if len(monthly_history) < 3:
            return
        last_month = monthly_history[-1][0]
        if last_month == next_date.replace(day=1):
            return
        counter = Counter()
        for h in monthly_history:
            counter.update(h[1])
        usual_day = usual_key(counter)
        try:
            expected = last_month.replace(month=last_month.month + 1, day=usual_day)
        except ValueError:
            if last_month.month == 12:
                expected = last_month.replace(year=last_month.year + 1, month=1, day=usual_day)
            else:
                expected = last_month.replace(month=last_month.month + 2, day=1) - datetime.timedelta(days=1)
        if expected < next_date:
            warn_date = expected
            while warn_date.month == expected.month and warn_date < next_date:
                yield (
                    warn_date,
                    f"{vendor_id} generally charges between on {usual_day} day of each month invoices are sent. On {expected}, an invoice from {vendor_id} has not been received",
                    'no_invoice_received',
                    'vendor',
                    None,
                    vendor_id,
                )
                warn_date += datetime.timedelta(days=1)

    quarterly_history = []
    def warn_quarterly(next_date: datetime.date):
        if len(quarterly_history) < 2:
            return
        last_quarter = quarterly_history[-1][0]
        if last_quarter == current_quarter(next_date):
            return
        counter = Counter()
        for h in quarterly_history:
            counter[h[1]] += 1
        usual_day = usual_key(counter)
        expected_month = last_quarter.month + 3
        expected_year = last_quarter.year
        if expected_month > 12:
            expected_month -= 12
            expected_year += 1
        try:
            expected = datetime.date(year=expected_year, month=expected_month, day=usual_day[1])
        except ValueError:
            if expected_month == 12:
                expected = datetime.date(year=expected_year + 1, month=1, day=1)
            else:
                expected = datetime.date(year=expected_year, month=expected_month + 1, day=1)
            expected -= datetime.timedelta(days=1)

        if expected < next_date:
            warn_date = expected
            while warn_date.month == expected.month and warn_date < next_date:
                yield (
                    warn_date,
                    f"{vendor_id} generally charges between on {usual_day[1]} day of each month invoices are sent. On {expected}, an invoice from {vendor_id} has not been received",
                    'no_invoice_received',
                    'vendor',
                    None,
                    vendor_id,
                )
                warn_date += datetime.timedelta(days=1)

    def warn(date: datetime.date):
        yield from warn_monthly(date)
        yield from warn_quarterly(date)

    for i in ins:
        yield from warn(i.invoice_date)

        month: datetime.date = i.invoice_date.replace(day=1)
        new_month = True
        if monthly_history:
            last_month, counter = monthly_history[-1]
            new_month = last_month != month
            if last_month < (month - datetime.timedelta(days=1)).replace(day=1):
                monthly_history.clear()
        if new_month:
            counter = Counter()
            monthly_history.append((month, counter))
        counter[i.invoice_date.day] += 1

        quarter = current_quarter(i.invoice_date)
        new_quarter = True
        if quarterly_history:
            last_quarter, day = quarterly_history[-1]
            new_quarter = last_quarter != quarter
            if not new_quarter or last_quarter < current_quarter(quarter - datetime.timedelta(days=1)):
                quarterly_history.clear()
        if new_quarter:
            day = (i.invoice_date.month - quarter.month, i.invoice_date.day)
            quarterly_history.append((quarter, day))

    yield from warn(datetime.date.today())


gleans_schema = StructType([
    StructField("glean_id", StringType(), False),
    StructField("glean_date", DateType(), False),
    StructField("glean_text", StringType(), False),
    StructField("glean_type", StringType(), False),
    StructField("glean_location", StringType(), False),
    StructField("invoice_id", StringType(), True),
    StructField("canonical_vendor_id", StringType(), False),
])

def main():
    spark = SparkSession.builder.appName("Invoice").getOrCreate()
    sc = spark.sparkContext

    invoices = spark.read.csv('data/invoice.csv', header=True, schema=invoice_schema).rdd
    line_items = spark.read.csv('data/line_item.csv', header=True, schema=line_item_schema).rdd

    invoices_has_date = invoices.filter(lambda i: i.invoice_date is not None).cache()

    invoices_grouped_by_vendor = (invoices_has_date
        .groupBy(lambda i: i.canonical_vendor_id)
        .cache()
    )

    vendor_not_seen_in_a_while = (invoices_grouped_by_vendor
        .flatMap(map_vendor_not_seen_in_a_while)
    )

    accrual_alert = (line_items
        .groupBy(lambda i: i.invoice_id)
        .join(invoices_has_date.keyBy(lambda i: i.invoice_id))
        .flatMap(map_accrual_alert)
    )

    large_month_increase_mtd = (invoices_grouped_by_vendor
        .flatMap(map_large_month_increase_mtd)
    )

    no_invoice_received = (invoices_grouped_by_vendor
        .flatMap(map_no_invoice_received)
    )

    gleans = (
        vendor_not_seen_in_a_while
        .union(accrual_alert)
        .union(large_month_increase_mtd)
        .union(no_invoice_received)
    ).map(lambda g: (str(uuid.uuid4()), *g))
    gleans.toDF(schema=gleans_schema).write.csv('data/gleans', header=True)

    sc.stop()

if __name__ == '__main__':
    main()
