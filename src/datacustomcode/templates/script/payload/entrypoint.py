from pyspark.sql.functions import col, upper

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode


def main():
    client = Client()

    df = client.read_dlo("Account_std__dll")

    # Perform transformations on the DataFrame
    df_upper1 = df.withColumn("description__c", upper(col("description__c")))

    """
    You can use your AI models configured in Salesforce to generate column values.
    For testing locally before deploying your code to Data Cloud (datacustomcode run),
    first configure an external client app before using LLM functionality, then configure
    the SDK with your client app credentials.
    
    https://developer.salesforce.com/docs/ai/agentforce/guide/agent-api-get-started.html#create-a-salesforce-app

    Example:

        >>> from datacustomcode.client import llm_gateway_generate_text_col
            df_generated = df.withColumn(
            ...     "Greeting__c",
            ...     llm_gateway_generate_text_col(
            ...         "In one sentence, greet {name} from {city}.",
            ...         {"name": col("Name__c"), "city": col("HomeCity__c")},
            ...         model_id="sfdc_ai__DefaultGPT4Omni",
            ...         max_tokens=100,
            ...     ),
            ... )

    You can also invoke the LLM with a literal plain text prompt — no
    ``{field}`` substitution is performed on this string.

    Example:

        >>> generated_text = client.llm_gateway_generate_text(prompt, model_id, max_tokens)
    """

    # Drop specific columns related to relationships
    df_upper1 = df_upper1.drop("sfdcorganizationid__c")
    df_upper1 = df_upper1.drop("kq_id__c")

    # Save the transformed DataFrame
    dlo_name = "Account_std_copy__dll"
    client.write_to_dlo(dlo_name, df_upper1, write_mode=WriteMode.APPEND)


if __name__ == "__main__":
    main()
