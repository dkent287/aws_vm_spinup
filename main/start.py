import boto3
import paramiko
import time
import os
import io
import zipfile
import json
import random
import pandas as pd

# Configuration parameters
r1 = str(random.randint(0, 101))
r2 = str(random.randint(0, 101))
r3 = str(random.randint(0, 101))
r4 = str(random.randint(0, 101))
tag_rand = r1 + r2 + r3 + r4
key_name = 'dtk-vm-spin-keypair-' + tag_rand

secret_name = 'secret-' + key_name

ami_id = 'ami-074be47313f84fa38'
bucket_name = 'dtk-bucket-vm-spin-' + tag_rand
data_file = 'data.csv'
instance_type = 't2.micro'
key_path = 'C:\\Users\\darre\\OneDrive\\Documents\\My Career\\Toolkit - Data Science\\Applications\\AWS VM Spinup\\' + key_name + '.pem'
lambda_function_name = 'StopEC2InstanceFunction'
lambda_layer_paramiko_arn = 'arn:aws:lambda:us-west-2:753005892268:layer:vm-spin-layer:7'
region = 'us-west-2'
script_file = 'train.py'
sns_topic_arn = 'arn:aws:sns:us-west-2:753005892268:MySNSTopic'

model_list = pd.read_csv('model_list.csv',index_col=None).squeeze().to_list()
model_list_file = 'model_list.csv'

# Initialize boto3 clients
ec2 = boto3.client('ec2', region_name=region)
s3 = boto3.client('s3', region_name=region)
events_client = boto3.client('events', region_name=region)
lambda_client = boto3.client('lambda', region_name=region)
iam = boto3.client('iam', region_name=region)
sns = boto3.client('sns', region_name=region)
secrets_client = boto3.client('secretsmanager', region_name=region)  # Replace with your region

# prepare and save finish key
finish_key = [bucket_name, key_name,secret_name]
finish_key = pd.DataFrame(finish_key)
finish_key.to_csv('finish_key.csv', index=False)

def create_key_pair(key_name):
    try:
        # Create a new key pair
        key_pair = ec2.create_key_pair(KeyName=key_name)
        return key_pair
    except Exception as e:
        print(f"Error creating key pair: {e}")
        return None

def save_secret_secretsmanager(secret_name, secret_value):
    try:
        # Save the secret to AWS Secrets Manager
        secrets_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(secret_value)
        )
        print(f"Secret {secret_name} successfully saved to Secrets Manager.")
    except Exception as e:
        print(f"Error saving secret to Secrets Manager: {e}")

def save_secret_local(secret_name):
    get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)
    private_key = secret.get('KeyMaterial')
    pem_file_name = key_name + '.pem'
    with open(pem_file_name, 'w') as pem_file:
        pem_file.write(private_key)

# Create an S3 bucket
def create_s3_bucket(bucket_name):
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
    print(f'S3 bucket {bucket_name} created')

# Create an EC2 instance
def create_ec2_instance():
    instance = ec2.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        KeyName=key_name,
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=[
        'sg-0eefbc2a32bc2dde8'],
    )
    instance_id = instance['Instances'][0]['InstanceId']
    print(f'EC2 instance {instance_id} created')
    return instance_id

# Wait for the instance to be running
def wait_for_instance(instance_id):
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])
    ec2_resource = boto3.resource('ec2', region_name=region)
    instance = ec2_resource.Instance(instance_id)
    instance.load()
    print(f'EC2 instance {instance_id} is running')
    return instance.public_dns_name

# Install necessary software on the EC2 instance
def install_software(public_dns):
    for i in range(5):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(public_dns, username='ec2-user', key_filename=key_path)
        except:
            time.sleep(10)
    commands = [
        'sudo yum update -y',
        'sudo dnf install python3.11 -y',
        'sudo dnf install python3.11-pip -y',
        'python3.11 -m pip install pandas scikit-learn'
    ]
    for command in commands:
        stdin, stdout, stderr = ssh.exec_command(command)
        print(stdout.read().decode())
        print(stderr.read().decode())
    ssh.close()
    print('Software installed')

# Upload files to the EC2 instance
def upload_files(public_dns):
    for i in range(5):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(public_dns, username='ec2-user', key_filename=key_path)
        except:
            time.sleep(10)
    sftp = ssh.open_sftp()
    sftp.put(data_file, f'/home/ec2-user/{os.path.basename(data_file)}')
    sftp.put(script_file, f'/home/ec2-user/{os.path.basename(script_file)}')
    sftp.put(model_list_file, f'/home/ec2-user/{os.path.basename(model_list_file)}')
    sftp.close()
    ssh.close()
    print('Files uploaded')

# Run the Python script on the EC2 instance using nohup
def run_script(public_dns):
    for i in range(5):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(public_dns, username='ec2-user', key_filename=key_path)
        except:
            time.sleep(10)
    command = f'nohup python3.11 /home/ec2-user/{os.path.basename(script_file)} &'
    ssh.exec_command(command)
    time.sleep(5)
    ssh.close()
    print('Python script running with nohup')

def create_lambda_role():
    iam = boto3.client('iam')
    
    assume_role_policy_document = json.dumps({
        'Version': '2012-10-17',
        'Statement': [
            {
                'Effect': 'Allow',
                'Principal': {
                    'Service': 'lambda.amazonaws.com'
                },
                'Action': 'sts:AssumeRole'
            }
        ]
    })
    
    # Create the role
    role_response = iam.create_role(
        RoleName='LambdaEC2FileCheckRole',
        AssumeRolePolicyDocument=assume_role_policy_document
    )
    
    # Attach policies
    policies = [
        'arn:aws:iam::aws:policy/AmazonEC2FullAccess',
        'arn:aws:iam::aws:policy/AmazonS3FullAccess',
        'arn:aws:iam::aws:policy/CloudWatchFullAccess',
        'arn:aws:iam::aws:policy/AmazonSNSFullAccess',
        'arn:aws:iam::aws:policy/SecretsManagerReadWrite',
        'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess'
    ]
    
    for policy_arn in policies:
        iam.attach_role_policy(
            RoleName='LambdaEC2FileCheckRole',
            PolicyArn=policy_arn
        )
    
    role_arn = role_response['Role']['Arn']
    
    return role_arn

def lamda_setup():
    # Create a simple Lambda function code
    lambda_function_code = f"""
import boto3
import paramiko
import os
import json
import pandas as pd

def lambda_handler(event, context):
    
    # retrieve key-pair
    secrets_client = boto3.client('secretsmanager', region_name='{region}')
    get_secret_value_response = secrets_client.get_secret_value(SecretId='{secret_name}')
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)
    private_key = secret.get('KeyMaterial')
    pem_file_path = '/tmp/' + '{key_name}' + '.pem'
    with open(pem_file_path, 'w') as pem_file:
         pem_file.write(private_key)
    
    # retrieve the public dns for the running ec2 instance
    client = boto3.client('ec2',region_name='{region}')
    response = client.describe_instances(InstanceIds=['{instance_id}'])
    reservations = response['Reservations']
    instances = reservations[0]['Instances']
    instance = instances[0]
    public_dns = instance.get('PublicDnsName', 'No Public DNS')

    # initiate SSH connection
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(public_dns, username='ec2-user', key_filename=pem_file_path)
    
    # check whether relevant files are present
    sftp = ssh.open_sftp()
    try:
        sftp.stat('/home/ec2-user/test_results.csv')
        file_present = True
    except FileNotFoundError:
        file_present = False
    
    # if relevant files are present, transfer files to S3, terminate EC2 and send SNS
    if file_present:
        sftp.get('test_results.csv', '/tmp/test_results.csv')
        sftp.get('model_list.csv', '/tmp/model_list.csv')
        
        model_list = pd.read_csv('/tmp/model_list.csv',index_col=None).squeeze().to_list()
        
        s3 = boto3.client('s3',region_name='{region}')
        s3.upload_file('/tmp/test_results.csv','{bucket_name}', 'test_results.csv')
        s3.upload_file('/tmp/model_list.csv','{bucket_name}', 'model_list.csv')
        
        for model in model_list:
            try:
                model_file_name = model + '.pkl'
                model_file_path = '/tmp/' + model_file_name
                sftp.get(model_file_name, model_file_path)
                s3.upload_file(model_file_path,'{bucket_name}', model_file_name)
            except:
                pass

        client.terminate_instances(InstanceIds=['{instance_id}'])
        
        sns = boto3.client('sns',region_name='{region}')
        sns.publish(
            TopicArn='{sns_topic_arn}',
            Message='All done.',
            Subject='EC2 Task Completion'
        )
    
        # Delete the Eventbridge rule and the associated target (i.e. the Lambda)
        events_client = boto3.client('events',region_name='{region}')
        target_details = events_client.list_targets_by_rule(Rule='TriggerLambdaEvery15Minutes')
        target_id = target_details['Targets'][0]['Id']
        response1 = events_client.remove_targets(Rule='TriggerLambdaEvery15Minutes', Ids=[target_id])
        response2 = events_client.delete_rule(Name='TriggerLambdaEvery15Minutes')
    
    sftp.close()
    ssh.close()
    
    return 'Check complete'
        """.format('region','secret_name','key_name','instance_id','bucket_name','sns_topic_arn')
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('lambda_function.py', lambda_function_code)
    zip_buffer.seek(0)
    
    return zip_buffer.read()

def create_lambda_function(function_name, lambda_zip, role_arn, handler='lambda_function.lambda_handler'):
    response = lambda_client.create_function(
        FunctionName=function_name,
        Runtime='python3.12',
        Role=role_arn,
        Handler=handler,
        Code={'ZipFile': lambda_zip},
        Description='A Lambda function that adds 1 + 1 and prints the result.',
        Timeout=15,
        MemorySize=128,
        Publish=True
    )
    return response

def attach_lambda_layer(function_name, lambda_layer_paramiko_arn, region_name=region):
        
    # Get the current function configuration to preserve existing layers
    response = lambda_client.get_function_configuration(FunctionName=lambda_function_name)
    
    # Extract existing layers
    existing_layers = response.get('Layers', [])
    existing_layer_arns = [layer['Arn'] for layer in existing_layers]
    new_layers = existing_layer_arns.copy()
    
    # Add the new layer ARN to the existing ones
    new_layers.append(lambda_layer_paramiko_arn)
    
    # Update the Lambda function configuration with the new layers
    lambda_client.update_function_configuration(
        FunctionName=function_name,
        Layers=new_layers
    )

def create_eventbridge_rule():
    # Create an EventBridge rule to trigger every 15 minutes
    response = events_client.put_rule(
        Name='TriggerLambdaEvery15Minutes',
        ScheduleExpression='rate(15 minutes)',
        State='ENABLED',
        Description='Trigger Lambda function every 15 minutes'
    )
    return response['RuleArn']

def add_lambda_permission(rule_arn):
    # Add permission to the Lambda function to be invoked by EventBridge
    lambda_client.add_permission(
        FunctionName=lambda_function_name,
        StatementId='AllowExecutionFromEventBridge',
        Action='lambda:InvokeFunction',
        Principal='events.amazonaws.com',
        SourceArn=rule_arn
    )

def add_target_to_rule(rule_arn):
    # Add the Lambda function as a target to the EventBridge rule
    response = events_client.put_targets(
        Rule='TriggerLambdaEvery15Minutes',
        Targets=[
            {
                'Id': '1',
                'Arn': lambda_client.get_function(FunctionName=lambda_function_name)['Configuration']['FunctionArn']
            }
        ]
    )
    return response


# Main execution flow

key_pair = create_key_pair(key_name)
if key_pair:
    # Prepare the key pair information to be saved
    key_pair_info = {
        'KeyName': key_pair['KeyName'],
        'KeyPairId': key_pair['KeyPairId'],
        'KeyFingerprint': key_pair['KeyFingerprint'],
        'KeyMaterial': key_pair['KeyMaterial']
    }
    
    # Save the key pair information to AWS Secrets Manager
    save_secret_secretsmanager(secret_name, key_pair_info)

save_secret_local(secret_name)
create_s3_bucket(bucket_name)
instance_id = create_ec2_instance()
public_dns = wait_for_instance(instance_id)
install_software(public_dns)
upload_files(public_dns)
run_script(public_dns)
role_arn = create_lambda_role()
time.sleep(120) 
lambda_zip = lamda_setup()
lambda_arn = create_lambda_function(lambda_function_name, lambda_zip, role_arn)
time.sleep(120) 
attach_lambda_layer(lambda_function_name, lambda_layer_paramiko_arn)

rule_arn = create_eventbridge_rule()
add_lambda_permission(rule_arn)
add_target_to_rule(rule_arn)

print("Go enjoy your day.")


