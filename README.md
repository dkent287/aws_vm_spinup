aws-vm-spinup

This is a solution for spinning up resources on AWS to train a machine learning model, downloading the results and then terminating the various services needed to train the model.

The AWS services include EC2, S3, Lambda, EventBridge, Secrets Manager, IAM and SNS.

The Lambda function is mainly responsible for checking whether the model builiding process has been completed and for performing some of the service shut-down work.  The Lambda function requires a Lambda layer which is built from a Docker container.  The folder aws_lambda_layer has a Dockerfile and instructions for building the .zip file which forms the input to the needed Lambda layer.  The code herein assumes that the Lambda layer is left in place after the initial set-up.

More detailed instructions on use to follow or available from the autthor.