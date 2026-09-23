from events import parse_events
from pyspark.sql.functions import col

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode


def main():
    client = Client()

    sessions = client.read_dlo("User_Sessions__dll").select(
        "session_id__c", "user_id__c", "events__c"
    )
    exploded = parse_events(sessions, "session_id__c", "user_id__c", "events__c")

    output = exploded.filter(col("event_id__c").isNotNull())

    client.write_to_dlo("User_Events__dll", output, WriteMode.OVERWRITE)


if __name__ == "__main__":
    main()
