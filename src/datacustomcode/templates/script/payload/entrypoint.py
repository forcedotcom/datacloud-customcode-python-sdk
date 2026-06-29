from pyspark.sql.functions import col, upper

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode


def main():
    client = Client()

    df = client.read_dlo("Account_std__dll")

    # Perform transformations on the DataFrame
    df_upper1 = df.withColumn("description__c", upper(col("description__c")))

    """
    You can use your AI models configured in Salesforce to generate column
    values. See README.md for how to test locally before deploying to Data Cloud.

    Example (the per-row helper returns a struct
    ``{status, response, error_code, error_message}`` — pick the field you
    want with ``[...]``):

        >>> from datacustomcode.client import llm_gateway_generate_text_col
            df_generated = df.withColumn(
            ...     "greeting__c",
            ...     llm_gateway_generate_text_col(
            ...         "In one sentence, greet {name} from {city}.",
            ...         {"name": col("name__c"), "city": col("homecity__c")},
            ...         model_id="sfdc_ai__DefaultGPT4Omni",
            ...     )["response"],
            ... )

    You can also invoke the LLM with a literal plain text prompt — no
    ``{field}`` substitution is performed on this string.

    Example:

        >>> generated_text = client.llm_gateway_generate_text(
        ...     prompt, model_id
        ... )

    You can also score rows with an Einstein Studio model. The per-row helper
    returns the same ``{status, response, error_code, error_message}`` struct,
    where ``response`` is the prediction payload as a JSON string.

    Example:

        >>> from datacustomcode.client import einstein_predict_col
        >>> from datacustomcode.einstein_predictions.types import PredictionType
            df_scored = df.withColumn(
            ...     "prediction__c",
            ...     einstein_predict_col(
            ...         "my_regression_model",
            ...         PredictionType.REGRESSION,
            ...         {"beds": col("beds__c"), "baths": col("baths__c")},
            ...     )["response"],
            ... )
    """

    # Drop specific columns related to relationships
    df_upper1 = df_upper1.drop("sfdcorganizationid__c")
    df_upper1 = df_upper1.drop("kq_id__c")

    # Save the transformed DataFrame
    dlo_name = "Account_std_copy__dll"
    client.write_to_dlo(dlo_name, df_upper1, write_mode=WriteMode.APPEND)


if __name__ == "__main__":
    main()
