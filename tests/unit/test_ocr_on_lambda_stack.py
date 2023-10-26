import aws_cdk as core
import aws_cdk.assertions as assertions

from ocr_on_lambda.ocr_on_lambda_stack import OcrOnLambdaStack

# example tests. To run these tests, uncomment this file along with the example
# resource in ocr_on_lambda/ocr_on_lambda_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = OcrOnLambdaStack(app, "ocr-on-lambda")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
