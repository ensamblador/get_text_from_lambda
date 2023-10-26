import sys

from aws_cdk import (aws_lambda, Duration, aws_iam as iam, CfnParameter)

from constructs import Construct
from layers import PyPDF2

LAMBDA_TIMEOUT= 60

BASE_LAMBDA_CONFIG = dict (
    timeout=Duration.seconds(LAMBDA_TIMEOUT),       
    memory_size=512,
    #architecture=aws_lambda.Architecture.ARM_64,
    tracing= aws_lambda.Tracing.ACTIVE)

PYTHON_LAMBDA_CONFIG = dict(runtime=aws_lambda.Runtime.PYTHON_3_11, **BASE_LAMBDA_CONFIG)



class Lambdas(Construct):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        pypdf = PyPDF2(self, 'PyPDF2')

        COMMON_LAMBDA_CONF = dict(**PYTHON_LAMBDA_CONFIG)
        
        self.pypdf_s3_file = aws_lambda.Function(
            self, "PyPDF", handler="lambda_function.lambda_handler",
            layers= [pypdf.layer],
            code = aws_lambda.Code.from_asset("./lambdas/code/pypdf_s3_file/"),**COMMON_LAMBDA_CONF)
        
        self.pypdf_s3_file.add_to_role_policy(iam.PolicyStatement( actions=["s3:GetObject", "s3:HeadObject", "s3:PutObject"], resources=['*']))
    

            
        self.textract_s3_file = aws_lambda.Function(
            self, "Textract", handler="lambda_function.lambda_handler",
            code = aws_lambda.Code.from_asset("./lambdas/code/textract_s3_file/"),**COMMON_LAMBDA_CONF)
        
        self.textract_s3_file.add_to_role_policy(iam.PolicyStatement( actions=["s3:GetObject", "s3:HeadObject", "s3:PutObject", "textract:*"], resources=['*']))
    