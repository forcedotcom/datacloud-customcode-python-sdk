from pyspark.sql.functions import col, upper

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode


def main():
    client = Client()

    df = client.read_dlo("Account_std__dll")

    # Perform transformations on the DataFrame
    df_upper1 = df.withColumn("Description__c", upper(col("Description__c")))

    # Drop specific columns related to relationships
    df_upper1 = df_upper1.drop("SfdcOrganizationId__c")
    df_upper1 = df_upper1.drop("KQ_Id__c")

    # Save the transformed DataFrame
    dlo_name = "Account_std_copy__dll"
    client.write_to_dlo(dlo_name, df_upper1, write_mode=WriteMode.APPEND)


if __name__ == "__main__":
    main()
