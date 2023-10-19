# Many to Many file ingestion
This is a modified version of Braze's file importer.  
This script is a template to implement many to many data flows from many sources to many destinations with a single lambda function.

THIS IS ONLY A TEMPLATE AND NOT A COMPLETE SOLUTION.  
There may be relics of the forked repository. This repo is purely for example purposes only.  


## Features

- Ingest a file in chunks.
- Create a custom handler for any source/destination.
- Skip attribute update by omitting a value for the given user
- Force a particular data type for a given attribute (most useful for phone numbers and zip codes)

## Implementation
Modify the `handlers.py` file to add your destination parsers (converts the destination file to a list of dicts) and your source handlers (converts a dict from the destination parser to the target payload).


#### API Key

To connect with Braze servers, we also need an API key. This unique identifier allows Braze to verify your identity and upload your data. To get your API key, open the Dashboard and scroll down the left navigation section. Select **Developer Console** under _App Settings_.

You will need an API key that has a permission to post to `user.track` API endpoint. If you know one of your API keys supports that endpoint, you can use that key. To create a new one, click on `Create New API Key` on the right side of your screen.

Next, name your API Key and select `users.track` under the _User Data_ endpoints group. Scroll down and click on **Save API Key**.
We will need this key shortly.


### Monitoring and Logging

#### CloudWatch

To make sure the function ran successfully, you can read the function's execution logs. Open the Braze User Import function (by selecting it from the list of Lambdas in the console) and navigate to **Monitor**. Here, you can see the execution history of the function. To read the output, click on **View logs in CloudWatch**. Select lambda execution event you want to check.

#### SNS

Optionally, you can publish a message to AWS SNS when the file is finished processing or it encounters a fatal error.

SNS message format:

    {
        // object key of the processed file
        "fileName": "abc",
        // true if file was processed with no fatal error
        "success": true,
        "usersProcessed": 123
    }

In order to use this feature, you must:

- [Update](#updating) the lambda function to version `0.2.2` or higher
- Allow Lambda to publish messages to the topic
- Set the `TOPIC_ARN` environment variable

To allow lambda to publish to the topic, head over to `Configuration -> Permissions` and under **Execution Role**, click on the Role name. Next, click `Add permissions -> Create inline policy`.

- Service: SNS
- Actions: Publish
- Resources: Under topic, Add ARN and specify the topic ARN

Review policy, add name `BrazeUserImportSNSPublish` and Create Policy.

Finally, set the lambda environment variable with key: **`TOPIC_ARN`** and provide the SNS topic ARN where you would like to publish the message as the value.

#### Lambda Configuration

By default, the function is created with 2048MB memory size. Lambda's CPU is proportional to the memory size. Even though, the script uses constant, low amount of memory, the stronger CPU power allows to process the file faster and send more requests simultaneously.  
2GB was chosen as the best cost to performance ratio.  
You can review Lambda pricing here: https://aws.amazon.com/lambda/pricing/.

You can reduce or increase the amount of available memory for the function in the Lambda **Configuration** tab. Under _General configuration_, click **Edit**, specify the amount of desired memory and save.

Keep in mind that any more memory above 2GB has diminishing returns where it might improve processing speed by 10-20% but at the same time doubling or tripling the cost.

<a name="updating"></a>

#### Updating an Existing Function

If you have already deployed the application and a new version is available in the repository, you can update by re-deploying the function as if you were doing it for the first time. That means you have to pass it the Braze API Key and Braze API URL again. The update will only overwrite the function code. It will not modify or delete other existing resources like the S3 bucket.

You can also upload the packaged `.zip` file that's available under [Releases](https://github.com/braze-inc/growth-shares-lambda-user-import/releases). In the AWS Console, navigate to the `braze-user-import` Lambda function and in the **Code** tab, click on **Upload from** and then **.zip file**. Select the downladed `.zip` file.

<a name="execution-times"></a>

## Estimated Execution Times

_2048MB Lambda Function_

| # of rows | Exec. Time |
| --------- | ---------- |
| 10k       | 3s         |
| 100k      | 30s        |
| 1M        | 5 min      |
| 5M        | 30 min     |

<br>

## Fatal Error

In case of an unexpected error that prevents further processing of the file, an event is logged (accessible through CloudWatch described in [Monitoring and Logging](#monitoring)) that can be used to restart the Lambda from the point where the program stopped processing the file. It is important not to re-import the same data to save Data Points. You can find the instructions how to do that below.

## Manual Triggers

In case you wanted to trigger the Lambda manually, for testing or due to processing error, you can do it from the AWS Lambda Console using a test event.  
Open the Braze User Import Lambda in the the AWS console by opening Lambda service and selecting `braze-user-import` function. Navigate to **Test**.

#### Event

If you have an event from a returned exception, paste it in the **Test event**. Otherwise, copy the contents of [`sample-event.json`](/events/sample-event.json). Replace the following values:

1. `"awsRegion"` under `"Records"`, replace `"your-region" with the proper region of the bucket with the file
2. `"name"` and `"arn"` under `"bucket"`, replace **only** `lambda-bucket-name` with the bucket name that files are read from (the bucket that triggers this Lambda)
3. `"key"` under `"object"` with the file key

_Optional_:

- `"offset"` field specifies the byte offset to start reading the file from
- `"headers"` field specifies headers and it is mandatory if the file is not being read from the beginning

#### Invoke

To invoke the function, press `Invoke` and wait for the execution to finish.

## Manual Function Deploy

<a name="role"></a>

### Role

The Lambda function requires permissions to read objects from S3, log to CloudWatch and call other Lambda functions. You can create a new role or add the policies to an existing roles.
Required policies:

    AmazonS3ReadOnlyAccess
    AWSLambdaBasicExecutionRole
    AWSLambdaRole

To create a new role with these permissions open [Roles](https://console.aws.amazon.com/iam/home?region=us-east-1#/roles) console.

1. Click **Create role**
2. Select **Lambda** as a use case, and click on **Next: Permissions**
3. Search and mark all policies mentioned above
4. Click **Next:Tags** and **Next:Review**, name your role and finally create it by pressing **Create role**

### Create Function

1. Download the packaged code from [Releases](https://github.com/braze-inc/growth-shares-lambda-user-import/releases)
2. Create a new [Lambda](https://console.aws.amazon.com/lambda/home?region=us-east-1#/discover) function.

   1. Select _Author from scratch_
   2. Name your function
   3. Select **Python 3.7** runtime
   4. Under **Change default execution role**, select _Use an existing role_ and select a role with all three policies described [above](#role)
   5. Create the function

3. Upload the packaged code downloaded from the repository by clicking on **Upload from** and selecting `.zip file`
4. Configure Lambda
   1. In the **Code** tab, scroll down to edit _Runtime settings_, changing Handler to `app.lambda_handler`
   2. In the **Configuration** tab, edit _General configuration_, setting timeout to `15` min and `0` sec, and changing Memory size to `2048` MB
   3. Also in **Configuration**, under _Environment variables_ add two key-value pairs: `BRAZE_API_URL` key with your API URL as value, and `BRAZE_API_KEY` with your API Key as value
   4. Under _Asynchronous invocation_, change `Retry attempts` to `0`.
5. Add an S3 trigger where you can drop the user attribute files by clicking on `+ Add trigger` under the Function overview, selecting **S3** as a trigger and the source bucket, optionally using a bucket prefix. Then Add the trigger.

# Contributing and Testing

In order to run tests, install

    pip install pytest pytest-mock pytest-env

And run

    pytest

Contributions are welcome.
