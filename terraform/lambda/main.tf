terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda"
  output_path = "${path.module}/lambda_function.zip"
}

data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

resource "aws_lambda_function" "lambda_function" {
  function_name = var.function_name
  filename      = data.archive_file.lambda_zip.output_path
  handler       = var.lambda_handler
  role          = data.aws_iam_role.lab_role.arn
  runtime       = "python3.13"

  # Optional but recommended
  description = "Processes incoming data files"
  timeout     = 60
  memory_size = 128
}

