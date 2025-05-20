from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
)

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode


def main():
    client = Client()

    employees = client.read_dlo("Employee__dll").persist()
    employees = employees.select("id__c", "manager_id__c", "name__c", "position__c")
    employees.show()
    employees_with_manager = (
        employees.alias("e")
        .join(
            employees.alias("m"),
            col("e.manager_id__c").cast("string") == col("m.id__c").cast("string"),
            "left",
        )
        .select(
            col("e.id__c"),
            col("e.name__c"),
            col("e.position__c"),
            col("e.manager_id__c"),
            col("m.name__c").alias("manager_name__c"),
        )
        .persist()
    )

    hierarchy_df = (
        employees_with_manager.filter(col("manager_id__c").isNull())
        .withColumn("hierarchy_level__c", lit(1))
        .withColumn("management_chain__c", col("name__c"))
        .persist()
    )

    current_level = 1

    while True:
        ewm = employees_with_manager.alias("ewm")
        hdf = hierarchy_df.filter(col("hierarchy_level__c") == current_level).alias(
            "hdf"
        )

        next_level_df = ewm.join(
            hdf,
            col("ewm.manager_id__c").cast("string") == col("hdf.id__c").cast("string"),
            "inner",
        ).select(
            col("ewm.id__c"),
            col("ewm.name__c"),
            col("ewm.position__c"),
            col("ewm.manager_id__c"),
            col("ewm.manager_name__c"),
            (col("hdf.hierarchy_level__c") + 1).alias("hierarchy_level__c"),
            concat_ws(" | ", col("hdf.management_chain__c"), col("ewm.name__c")).alias(
                "management_chain__c"
            ),
        )

        if next_level_df.isEmpty():
            break

        hierarchy_df = hierarchy_df.union(next_level_df).persist()
        current_level += 1

    hierarchy_df = hierarchy_df.orderBy("hierarchy_level__c", "manager_id__c", "id__c")

    dlo_name = "Employee_Hierarchy__dll"
    client.write_to_dlo(dlo_name, hierarchy_df, WriteMode.APPEND)


if __name__ == "__main__":
    main()
