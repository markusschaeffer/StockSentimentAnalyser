import boto3

REGION = 'eu-central-1'
IMAGEID = 'ami-0ec1ba09723e5bfac'
INSTANCETYPE = 't2.micro'
KEYNAME = 'aws_ssh_frankfurt'

USERDATA = """
#!/bin/bash 

########## START OF PUBLIC INSTALLATION SCRIPT ##########
#install git and cd into project (install into /)
sudo yum -y install git
cd /
git clone https://github.com/markusschaeffer/StockSentimentAnalyser.git
cd /StockSentimentAnalyser

#download and install anaconda
wget https://repo.continuum.io/archive/Anaconda3-2019.07-Linux-x86_64.sh  
bash Anaconda3-2019.07-Linux-x86_64.sh -b -p

#add conda to PATH variable
export PATH=/anaconda3/bin:$PATH

#create conda environment (python 3.6.8)
cd /StockSentimentAnalyser
conda env create -f environment.yml

#activate bash shell for conda (conda init bash)
#https://github.com/conda/conda/issues/7980
source /anaconda3/etc/profile.d/conda.sh

#activate conda environment
conda activate sent-env

cd /StockSentimentAnalyser
#install pip dependencies
pip install -r requirements.txt

#install nltk dependencies
python nltk_setup.py

########## END OF PUBLIC INSTALLATION SCRIPT ##########
# TODO
#1.) add config.py to /StockSentimentAnalyser
    # twitter
    #CONSUMER_KEY = ''
    #CONSUMER_SECRET = ''
    #ACCESS_TOKEN = ''
    #ACCESS_TOKEN_SECRET = ''
    #s3
    #MODEL_BUCKET_NAME = '541304926041-classifier-model' # replace with your bucket name
    #MODEL_FILE_NAME = 'latest-model.pk' # replace with your object key
#2.) add your .aws folder (region and credentials) to ~

# Start main.py
cd /StockSentimentAnalyser
#nohup = keep running after ssh connection closed 
nohup python main.py &

"""

ec2 = boto3.resource('ec2', region_name=REGION)

def lambda_handler(event, context):
    try:
        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.start_instances
        instance = ec2.create_instances(
                ImageId=IMAGEID, 
                InstanceType=INSTANCETYPE,
                MinCount=1, 
                MaxCount=1, 
                KeyName=KEYNAME, 
                UserData=USERDATA,
                TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': 'StockSentimentAnalyser'}]}]
                )
        print(instance[0])
    except Exception as e:
        print(e)
