variable "aws_region" {
  default = "us-east-1"
}

variable "bypass_state_bucket_name" {
  description = "Bucket de State do Terraform"
  type        = string
  default     = "bypass-state-bucket"
}

variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
  default     = "bypass-lambda"
}

variable "lambda_handler" {
  description = "Handler for the Lambda function"
  type        = string
  default     = "main.lambda_handler"  
}
