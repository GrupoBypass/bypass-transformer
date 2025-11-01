provider "aws" {
  region = var.aws_region
}

terraform {
  backend "s3" {
    bucket = "bypass-terraform-backup"
    key    = "terraform/state.tfstate"
    region = "us-east-1"
  }
}

resource "aws_security_group" "ssh_sg" {
  name        = "ssh-access"
  description = "Allow SSH inbound traffic"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Server"
    from_port   = 5000
    to_port     = 5000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_s3_bucket" "raw" {
  bucket = "${var.bucket_name}-raw"
}

resource "aws_s3_bucket_versioning" "versioning-raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "trusted" {
  bucket = "${var.bucket_name}-trusted"
}

resource "aws_s3_bucket_versioning" "versioning-trusted" {
  bucket = aws_s3_bucket.trusted.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "client" {
  bucket = "${var.bucket_name}-client"
}

resource "aws_s3_bucket_versioning" "versioning-client" {
  bucket = aws_s3_bucket.client.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_instance" "transformer" {
  ami                    = "ami-0341d95f75f311023"
  instance_type          = "t2.medium"
  key_name               = "bypass-key"
  vpc_security_group_ids = [aws_security_group.ssh_sg.id]

  tags = {
    Name = "bypass-transformer-ec2"
  }
}

resource "aws_dynamodb_table" "circuito" {
  name         = "Circuito"
  billing_mode = "PAY_PER_REQUEST"   # equivalente ao modo on-demand
  hash_key     = "sensor_id"

  attribute {
    name = "sensor_id"
    type = "S"
  }

  table_class = "STANDARD"

  tags = {
    Environment = "prod"
  }
}

resource "aws_dynamodb_table" "sensor_metadata" {
  name         = "SensorMetadata"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "sensor_id"

  attribute {
    name = "sensor_id"
    type = "S"
  }

  table_class = "STANDARD"

  tags = {
    Environment = "prod"
  }
}

resource "aws_dynamodb_table" "piezo_sensor_distancia" {
  name         = "PiezoSensorDistancia"
  billing_mode = "PAY_PER_REQUEST"   # equivalente ao modo on-demand
  hash_key     = "sensor_id"

  attribute {
    name = "sensor_id"
    type = "S"
  }

  table_class = "STANDARD"

  tags = {
    Environment = "prod"
  }
}

resource "aws_dynamodb_table" "tof_data" {
  name         = "TofData"
  billing_mode = "PAY_PER_REQUEST" # modo on-demand
  hash_key     = "sensor_id"
  range_key    = "datahora"

  attribute {
    name = "sensor_id"
    type = "S"
  }

  attribute {
    name = "datahora"
    type = "S"
  }

  table_class = "STANDARD"

  tags = {
    Environment = "prod"
  }
}

resource "aws_dynamodb_table" "piezo_data" {
  name         = "PiezoData"
  billing_mode = "PAY_PER_REQUEST" # modo on-demand
  hash_key     = "trem_id"

  attribute {
    name = "trem_id"
    type = "S"
  }

  table_class = "STANDARD"

  tags = {
    Environment = "prod"
  }
}

resource "aws_dynamodb_table" "dps_data" {
  name         = "DpsData"
  billing_mode = "PAY_PER_REQUEST" # modo on-demand
  hash_key     = "sensor_id"
  range_key    = "datahora"

  attribute {
    name = "sensor_id"
    type = "S"
  }

  attribute {
    name = "datahora"
    type = "S"
  }

  table_class = "STANDARD"

  tags = {
    Environment = "prod"
  }
}

resource "aws_dynamodb_table" "dht11_data" {
  name         = "DHT11Data"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "sensor_id"
  range_key    = "datahora"

  attribute {
    name = "sensor_id"
    type = "S"
  }

  attribute {
    name = "datahora"
    type = "S"
  }

  table_class = "STANDARD"

  tags = {
    Environment = "prod"
  }
}

output "aws_dynamodb_table_circuito_name" {
  value = aws_dynamodb_table.circuito.name
}

output "aws_dynamodb_table_sensor_metadata_name" {
  value = aws_dynamodb_table.sensor_metadata.name
}

output "aws_dynamodb_table_piezo_sensor_distancia_name" {
  value = aws_dynamodb_table.piezo_sensor_distancia.name
}

output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "trusted_bucket_name" {
  value = aws_s3_bucket.trusted.bucket
}

output "client_bucket_name" {
  value = aws_s3_bucket.client.bucket
}

output "ec2_public_ip" {
  value = aws_instance.transformer.public_ip
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "trusted_bucket_arn" {
  value = aws_s3_bucket.trusted.arn
}

output "client_bucket_arn" {
  value = aws_s3_bucket.client.arn
}
