terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # LEAVE THIS EMPTY! The YAML file will fill this in automatically.
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region
}
