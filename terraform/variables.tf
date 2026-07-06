variable "aws_region" {
  description = "The AWS Region to deploy to"
  type        = string
  default     = "ap-south-1"
}

variable "instance_type" {
  description = "The EC2 instance type (t3.medium: 2 vCPUs, 4GB RAM — minimum for Kubernetes)"
  type        = string
  default     = "t3.medium"
}

variable "key_name" {
  description = "Name of an existing AWS SSH key pair to access the instance"
  type        = string
  default     = "nexusiot-key"
}
