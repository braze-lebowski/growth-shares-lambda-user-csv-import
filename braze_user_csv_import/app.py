"""This lambda function processes a CSV file to update user attributes within
Braze by using Braze API `users/track <https://www.braze.com/docs/api/endpoints/user_data/post_user_track/>`_
endpoint.

The expected CSV format is:
`external_id,attr_1,...attr_N` -- where the first column specifies external_id
of the user to be updated and every column afterwards specifies an attribute
to update.

The lambda will run up for to 10 minutes. If the file is not processed until
then, it will automatically deploy another lambda to continue processing
the file from where it left off.

The CSV file is streamed by 10MB chunks. User updates are posted to Braze
platform as the processing goes on, in 75 user chunks which is the maximum
amount of users supported by the Braze API.
"""

import csv
import os
import ast
import json
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict, Iterator, List, Optional, Sequence, Type, Union

import requests
import boto3
from urllib.parse import unquote_plus
from requests.exceptions import RequestException

from tenacity import (
    RetryCallState,
    retry,
    stop_after_attempt,  # type: ignore
    wait_exponential,  # type: ignore
    retry_if_exception_type  # type: ignore
)

from . import handlers


# 10 minute function timeout
FUNCTION_RUN_TIME = 10 * 60 * 1_000

# Remaining time threshold to end execution. 900_000 seconds is equal to
# 15 minutes which is the maximum lambda execution time
FUNCTION_TIME_OUT = 900_000 - FUNCTION_RUN_TIME
MAX_THREADS = 20
MAX_RETRIES = 5

BRAZE_API_URL = os.environ['BRAZE_API_URL']
BRAZE_API_KEY = os.environ['BRAZE_API_KEY']

if BRAZE_API_URL[-1] == '/':
    BRAZE_API_URL = BRAZE_API_URL[:-1]

TypeMap = Dict[str, Type]


def lambda_handler(event, context):
    """Receives S3 file upload event and starts processing the file.

    :param event: Event object containing information about the invoking service
    :param context: Context object, passed to lambda at runtime, providing
                    information about the function and runtime environment
    """
    print("New CSV to Braze import process started")
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = unquote_plus(event['Records'][0]['s3']['object']['key'])

    print(f"Processing {bucket_name}/{object_key}")
    file_processor = FileProcessor(
        bucket_name,
        object_key,
        event.get("offset", 0),
        event.get("source", None),
    )

    try:
        file_processor.process_file()
    except Exception as e:
        fatal_event = _create_event(
            event,
            file_processor.total_offset,
            file_processor.headers
        )
        _handle_fatal_error(e, file_processor.processed_rows,
                            fatal_event, object_key)
        raise

    print(f"Processed {file_processor.processed_rows:,} rows")
    if not file_processor.is_finished():
        _start_next_process(
            context.function_name,
            event,
            file_processor.total_offset,
            file_processor.headers
        )
    else:
        print(f"File {object_key} import is complete")

    _publish_message(object_key, True, file_processor.processed_rows)
    return {
        "users_processed": file_processor.processed_rows,
        "bytes_read": file_processor.total_offset - event.get("offset", 0),
        "is_finished": file_processor.is_finished()
    }


class FileProcessor:
    """Class responsible for reading and processing the CSV file, and delegating
    the user attribute update to background threads.

    :param bucket_name: S3 bucket name to get the object from
    :param object_key: S3 object key of the CSV file to process
    :param offset: Amount of bytes read already from the file
    :param source: a string which determines which processor to use to parse the data.
    """

    def __init__(
        self,
        bucket_name: str,
        object_key: str,
        offset: int = 0,
        source: Optional[str] = None,
    ) -> None:
        self.processing_offset = 0
        self.total_offset = offset
        self.file = _get_file_from_s3(bucket_name, object_key)
        self.source = source or handlers.determine_source(bucket_name, object_key)

        self.processed_rows = 0


    def process_file(self) -> None:
        """Processes the file.

        It reads the file by 10MB chunks.
        It parsers the chunk using a defined parser based on the source.
        It converts each row to a users/track payload.
        It groups the payloads to minimise requests made.
        It makes the requests to the users/track endpoint.
        """
        for chunk in self.iter_lines():
            parser = handlers.get_parser(self.source)
            rows: List[Dict] = parser(chunk)
            payloads = defaultdict(list)
            for row in rows:
                destination = handlers.determine_destination(row)
                row_handler = handlers.get_row_handler(destination)
                payload = row_handler(payload)
                payloads[destination].append(payload)

            grouped_payloads = handlers.chunk_grouper(payloads)
        
            for destination_key, payloads in grouped_payloads.items():
                self.post_users(destination_key, payloads)
        return


    def iter_lines(self) -> Iterator:
        """Iterates over lines in the object.

        Reads chunks of data (10MB) by default, and splits it into lines.
        Yields each line separately.
        """
        chunk_size = 1024*1024*10
        object_stream = _get_object_stream(self.csv_file, self.total_offset)
        leftover = b''
        for chunk in object_stream.iter_chunks(chunk_size=chunk_size):
            data = leftover + chunk

            # Current chunk is not the end of the file
            if len(data) + self.total_offset < self.csv_file.content_length:
                last_newline = data.rfind(b'\n')
                data, leftover = data[:last_newline], data[last_newline:]

            rows = []
            for line in data.splitlines(keepends=True):
                self.processing_offset += len(line)
                rows.append(line.decode("utf-8"))
            
            yield rows

        # Last empty new line in the file
        if leftover == b'\n':
            self.total_offset += len(leftover)

    def post_users(self, destination_key, api_payload_chunks: List[List[Dict]]) -> None:
        """Posts updated users to Braze platform using Braze API.

        :param api_payload_chunks: List of chunked payloads
        """
        updated = _post_users(destination_key, api_payload_chunks)
        self.processed_rows += updated
        self._move_offset()

    def is_finished(self) -> bool:
        """Returns whether the end of file was reached or there were no rows in the file."""
        return not self.processed_rows or not self.total_offset or self.total_offset >= self.csv_file.content_length

    def _move_offset(self) -> None:
        self.total_offset += self.processing_offset
        self.processing_offset = 0


def _get_file_from_s3(bucket_name: str, object_key: str):
    """Returns the S3 Object with `object_key` name, from `bucket_name` S3
    bucket."""
    return boto3.resource("s3").Object(bucket_name, object_key)  # type: ignore


def _get_object_stream(s3_object, offset: int):
    """Returns an object stream from the S3 file, starting at the specified
    offset.

    :param object: Object returned from S3 resource
    :param offset: Byte file offset
    :return: Stream of the S3 object, starting from ``offset``.
    """
    return s3_object.get(Range=f"bytes={offset}-")["Body"]


def _post_users(destination_key: str, api_payloads: List[Dict]) -> int:
    """Posts users concurrently to Braze API, using `MAX_THREADS` concurrent
    threads.

    In case of a server error, or in case of Too Many Requests (429)
    client error, the function will employ exponential delay stall and try
    again.

    :return: Number of users successfully updated
    """
    updated = 0
    payloads = []
    for p in api_payloads:
        payloads.append({
            'destination_key': destination_key,
            'payload': p,
        })
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results = executor.map(_post_to_braze, api_payloads)
        for result in results:
            updated += result
    return updated


def _on_network_retry_error(state: RetryCallState):
    print(
        f"INFO: Retry attempt: {state.attempt_number}/{MAX_RETRIES}. Wait time: {state.idle_for}")


@retry(retry=retry_if_exception_type(RequestException),
       wait=wait_exponential(multiplier=5, min=5),
       stop=stop_after_attempt(MAX_RETRIES),
       after=_on_network_retry_error,
       reraise=True)
def _post_to_braze(params: Dict) -> int:
    """Posts users read from the file to Braze users/track API endpoint.

    Authentication is necessary. Braze Rest API key is expected to be passed
    to the lambda process as an environment variable, under `BRAZE_API_KEY` key.
    In case of a lack of valid API Key, the function will fail.

    Each request is retried 3 times. This retry session takes place in each
    thread individually and it is independent of the global retry strategy.

    :return: The number of users successfully imported
    """
    destination_key = params['destination_key']
    api_payload = params['api_payload']
    api_url = os.environ[f'{destination_key}__BRAZE_API_URL']
    api_key = os.environ[f'{destination_key}__BRAZE_API_KEY']
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "X-Braze-Bulk": "true"
    }
    response = requests.post(
        f"https://{api_url}/users/track",
        headers=headers,
        json=api_payload
    )
    n_payloads = sum(
        len(api_payload['attributes']),
        len(api_payload['purchases']),
        len(api_payload['events']),
    )
    error_users = _handle_braze_response(response)
    return len(n_payloads) - error_users


def _handle_braze_response(response: requests.Response) -> int:
    """Handles server response from Braze API.

    The amount of requests made is well
    below the limits for the given API endpoint therefore Too Many Requests
    API errors are not expected. In case they do, however, occur - the API
    calls will be re-tried, up to `MAX_API_RETRIES`, using exponential delay.
    In case of a server error, the same strategy will be applied. After max
    retries have been reached, the execution will terminate.

    In case users were posted but there were minor mistakes, the errors will be
    logged. In case the API received data in an unexpected format, the data
    that caused the issue will be logged.

    In any unexpected client API error (other than 400), the function execution
    will terminate.

    :param response: Response from the API
    :return: Number of users that resulted in an error
    :raise APIRetryError: On a 429 or 500 server error
    :raise FatalAPIError: After `MAX_API_RETRIES` unsuccessful retries, or on
                          any non-400 client error
    """
    res_text = json.loads(response.text)
    if response.status_code == 201 and 'errors' in res_text:
        print(
            f"Encountered errors processing some users: {res_text['errors']}")
        return len(res_text['errors'])

    if response.status_code == 400:
        print(f"Encountered error for user chunk. {response.text}")
        return 0

    server_error = response.status_code == 429 or response.status_code >= 500
    if server_error:
        raise APIRetryError("Server error. Retrying..")

    if response.status_code > 400:
        raise FatalAPIError(res_text.get('message', response.text))

    return 0


def _start_next_process(function_name: str, event: Dict, offset: int,
                        headers: Optional[Sequence[str]]) -> None:
    """Starts a new lambda process, passing in current offset in the file.

    :param function_name: Python function name for Lambda to invoke
    :param event: Received S3 event object
    :param offset: The amount of bytes read so far
    :param headers: The headers in the CSV file
    """
    print("Starting new user processing lambda..")
    new_event = _create_event(event, offset, headers)
    boto3.client("lambda").invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps(new_event),
    )


def _create_event(received_event: Dict, byte_offset: int,
                  source: Optional[str]) -> Dict:
    return {
        **received_event,
        "offset": byte_offset,
        "source": source
    }


def _should_terminate(context) -> bool:
    """Returns whether lambda should terminate execution."""
    return context.get_remaining_time_in_millis() < FUNCTION_TIME_OUT


def _publish_message(file_name: str, success: bool, users_processed: int) -> None:
    """Publishes a message to AWS SNS.

    :param file_name: Name of the CSV file processed
    :param result: Result of the processing, 'success' or 'fail'
    :param users_processed: Number of users sent to Braze successfully
    """
    topic_arn = os.environ.get('TOPIC_ARN')
    if not topic_arn:
        print("No topic ARN provided. Skipping publishing a message")
        return

    print("Publishing a message to SNS")
    sns_client = boto3.client('sns')
    message = json.dumps({
        "fileName": file_name,
        "success": success,
        "usersProcessed": users_processed
    })

    sns_client.publish(
        TargetArn=topic_arn,
        Message=message
    )


def _handle_fatal_error(
    error: Exception,
    processed_rows: int,
    event: Dict,
    file_name: str
) -> None:
    """Prints logging information when a fatal error occurred."""
    print(f'Encountered error: "{error}"')
    print(f"Processed {processed_rows:,} users")
    print(f"Use the event below to continue processing the file:")
    print(json.dumps(event))

    _publish_message(file_name, False, processed_rows)



class APIRetryError(RequestException):
    """Raised on 429 or 5xx server exception. If there are retries left, the
    API call will be made again after a delay."""
    pass


class FatalAPIError(Exception):
    """Raised when received an unexpected error from the server. Causes the
    execution to fail."""
    pass
