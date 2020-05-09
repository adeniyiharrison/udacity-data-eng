import os
import json
import time
import boto3
import logging
import configparser
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s : %(message)s"
    )

class createCluster:
    def __init__(
        self
    ):
        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))

        # Ensure UDACITY KEY and SECRETs have been set in bash_profile or using export
        self.KEY = os.getenv("UDACITY_KEY")
        self.SECRET = os.getenv("UDACITY_SECRET")
        self.DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
        self.DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
        self.DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")
        self.DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        self.DWH_DB = config.get("DWH","DWH_DB")
        self.DWH_DB_USER = config.get("DWH","DWH_DB_USER")
        self.DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
        self.DWH_PORT = config.get("DWH","DWH_PORT")
        self.DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")


    def run(
        self
    ):
        self.create_clients()
        self.create_iam_role()
        self.create_cluster()
        self.update_security_group()

    def create_clients(
        self,
        ec2=True,
        s3=True,
        iam=True,
        redshift=True
    ):
        if ec2:
            self.ec2 = boto3.client(
                "ec2",
                region_name="us-west-2",
                aws_access_key_id=self.KEY,
                aws_secret_access_key=self.SECRET
            )

        if s3:
            self.s3 = boto3.client(
                "s3",
                region_name="us-west-2",
                aws_access_key_id=self.KEY,
                aws_secret_access_key=self.SECRET
            )

        if iam:
            self.iam = boto3.client(
                "iam",
                region_name="us-west-2",
                aws_access_key_id=self.KEY,
                aws_secret_access_key=self.SECRET
            )

        if redshift:
            self.redshift = boto3.client(
                "redshift",
                region_name="us-west-2",
                aws_access_key_id=self.KEY,
                aws_secret_access_key=self.SECRET
            )

        logging.info("Clients are ready")
    
    def create_iam_role(
        self
    ):

        assume_role_doc = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "redshift.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        })

        existing_role_list = []
        for x in self.iam.list_roles()["Roles"]:
            existing_role_list.append(x["RoleName"])
        
        if self.DWH_IAM_ROLE_NAME not in existing_role_list:
            dwhRole = self.iam.create_role(
                RoleName=self.DWH_IAM_ROLE_NAME,
                Description="Allow redshift to read from S3 Bucket",
                AssumeRolePolicyDocument=assume_role_doc
            )

            self.iam.attach_role_policy(
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
                RoleName=self.DWH_IAM_ROLE_NAME
            )

        self.DWH_ROLE_ARN = self.iam.get_role(
            RoleName=self.DWH_IAM_ROLE_NAME 
        )["Role"]["Arn"]

        logging.info("Roles are ready")

    def create_cluster(
        self
    ):

        existing_cluster_list = []
        for x in self.redshift.describe_clusters()["Clusters"]:
            existing_cluster_list.append(x["ClusterIdentifier"])

        if self.DWH_CLUSTER_IDENTIFIER in existing_cluster_list:
            raise Exception(
                "{} already exists!".format(self.DWH_CLUSTER_IDENTIFIER)
            )

        response = self.redshift.create_cluster(
            # add parameters for hardware
            DBName=self.DWH_DB,
            ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
            ClusterType=self.DWH_CLUSTER_TYPE,
            NodeType=self.DWH_NODE_TYPE,
            NumberOfNodes=int(self.DWH_NUM_NODES),
            # add parameters for identifiers & credentials
            MasterUsername=self.DWH_DB_USER,
            MasterUserPassword=self.DWH_DB_PASSWORD,
            # add parameter for role (to allow s3 access)
            IamRoles=[
            self.DWH_ROLE_ARN
            ]
        )

        describe_dict = self.redshift.describe_clusters(
            ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]

        while describe_dict["ClusterStatus"] != "available":
            logging.info("Cluster still is unavailable.. sleeping for 1 minute")
            time.sleep(60)
            describe_dict = self.redshift.describe_clusters(
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER
            )["Clusters"][0]
        
        self.DWH_ENDPOINT = describe_dict["Endpoint"]["Address"]
        logging.info("Cluster is ready")
    

    def update_security_group(
        self
    ):

        try:
            defaultSg = self.ec2.describe_security_groups(
                GroupNames=["default"]
            )["SecurityGroups"][0]["GroupId"]

            responseSg = self.ec2.authorize_security_group_ingress(
                IpProtocol="TCP",
                CidrIp="0.0.0.0/0",
                FromPort=int(self.DWH_PORT),
                ToPort=int(self.DWH_PORT),
                GroupId=defaultSg
            )
        except Exception as e:
            return e
        
if __name__ == "__main__":
    cc = createCluster()
    cc.run()