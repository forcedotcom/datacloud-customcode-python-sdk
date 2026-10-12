from pyspark.sql.functions import (
    col,
    concat_ws,
    sum as _sum,
)

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode


def main():
    client = Client()

    payments = client.read_dlo("Payment__dll").select(
        "customer_id__c", "amount__c", "cdp_sys_record_currency__c"
    )

    totals = (
        payments.groupBy("customer_id__c", "cdp_sys_record_currency__c")
        .agg(_sum("amount__c").alias("total_amount__c"))
        .select(
            concat_ws(
                "_", col("customer_id__c"), col("cdp_sys_record_currency__c")
            ).alias("id__c"),
            col("customer_id__c"),
            col("total_amount__c"),
            col("cdp_sys_record_currency__c"),
        )
    )

    client.write_to_dlo("Customer_Payment_Totals__dll", totals, WriteMode.OVERWRITE)


if __name__ == "__main__":
    main()
