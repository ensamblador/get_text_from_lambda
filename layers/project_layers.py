import json
from constructs import Construct

from aws_cdk import (
    aws_lambda as _lambda

)



class PyPDF2(Construct):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        pypdf = _lambda.LayerVersion(
            self, "PyPDF2", code=_lambda.Code.from_asset("./layers/pypdf2.zip"),
            compatible_runtimes = [
                _lambda.Runtime.PYTHON_3_8,
                _lambda.Runtime.PYTHON_3_9,
                _lambda.Runtime.PYTHON_3_10,
                _lambda.Runtime.PYTHON_3_11], 
            description = 'Pypdf')

        
        self.layer = pypdf



