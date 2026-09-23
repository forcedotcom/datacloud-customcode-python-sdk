from pyspark.sql.functions import col, sum as _sum, coalesce, lit

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode


def main():
    client = Client()

    accounts = (
        client.read_dlo("Account__dll")
        .select("id__c", "parent_id__c", "arr__c")
        .persist()
    )

    descendants = accounts.select(
        col("id__c").alias("root_id"),
        col("id__c").alias("descendant_id"),
    ).persist()

    frontier = descendants
    while True:
        next_hop = (
            frontier.alias("f")
            .join(
                accounts.alias("a"),
                col("a.parent_id__c") == col("f.descendant_id"),
                "inner",
            )
            .select(
                col("f.root_id").alias("root_id"),
                col("a.id__c").alias("descendant_id"),
            )
        )
        if next_hop.isEmpty():
            break
        descendants = descendants.union(next_hop).persist()
        frontier = next_hop

    totals = (
        descendants.alias("d")
        .join(
            accounts.alias("a"),
            col("d.descendant_id") == col("a.id__c"),
            "left",
        )
        .select(col("d.root_id"), col("a.arr__c"))
        .groupBy("root_id")
        .agg(coalesce(_sum("arr__c"), lit(0)).alias("tree_arr__c"))
        .withColumnRenamed("root_id", "id__c")
    )

    client.write_to_dlo("Account_Rollup__dll", totals, WriteMode.OVERWRITE)


if __name__ == "__main__":
    main()
