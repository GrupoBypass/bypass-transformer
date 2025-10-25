provider "aws" {
  region = var.aws_region
}

terraform {
  backend "s3" {
    bucket = "bypass-terraform-state"
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

