"""
Local test runner for the Lambda function.
This script allows you to test the Lambda function locally.
"""
import os
from dotenv import load_dotenv
from lambda_function import main

# Load environment variables from .env file
load_dotenv()

# Create a mock Lambda event and context
class MockContext:
    """Mock AWS Lambda context object"""
    def __init__(self):
        self.function_name = "reltio-sfdc-sync"
        self.function_version = "$LATEST"
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:reltio-sfdc-sync"
        self.memory_limit_in_mb = 512
        self.aws_request_id = "test-request-id"
        self.log_group_name = "/aws/lambda/reltio-sfdc-sync"
        self.log_stream_name = "test-stream"

# Mock event (empty dict is fine for this function)
event = {}

# Create mock context
context = MockContext()

if __name__ == "__main__":
    print("=" * 80)
    print("Running Lambda function locally...")
    print("=" * 80)
    print()
    
    try:
        # Run the main function
        result = main(event, context)
        
        print()
        print("=" * 80)
        print("Function execution completed!")
        print("=" * 80)
        print(f"Status Code: {result.get('statusCode', 'N/A')}")
        print(f"Body: {result.get('body', 'N/A')}")
    except Exception as e:
        print()
        print("=" * 80)
        print("ERROR: Function execution failed!")
        print("=" * 80)
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        print()
        print("Full traceback:")
        import traceback
        traceback.print_exc()
