variable "aws_region" {
  description = "The AWS Region to deploy to"
  type        = string
  default     = "ap-south-1"
}

variable "instance_type" {
  description = "The EC2 instance type (t3.micro is AWS free-tier eligible)"
  type        = string
  default     = "t3.micro"
}

variable "key_name" {
  description = "Name of an existing AWS SSH key pair to access the instance"
  type        = string
  default     = "nexusiot-key"
}
