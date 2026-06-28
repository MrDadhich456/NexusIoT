terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# 1. Custom VPC for isolated, reproducible networking
resource "aws_vpc" "nexusiot_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "nexusiot-vpc"
  }
}

# 2. Internet Gateway to connect VPC to the internet
resource "aws_internet_gateway" "nexusiot_igw" {
  vpc_id = aws_vpc.nexusiot_vpc.id

  tags = {
    Name = "nexusiot-igw"
  }
}

# 3. Public Subnet for the EC2 Instance
resource "aws_subnet" "nexusiot_public_subnet" {
  vpc_id                  = aws_vpc.nexusiot_vpc.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true
  availability_zone       = "${var.aws_region}a"

  tags = {
    Name = "nexusiot-public-subnet"
  }
}

# 4. Route Table for Internet Routing
resource "aws_route_table" "nexusiot_rt" {
  vpc_id = aws_vpc.nexusiot_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.nexusiot_igw.id
  }

  tags = {
    Name = "nexusiot-rt"
  }
}

# 5. Route Table Association
resource "aws_route_table_association" "nexusiot_rta" {
  subnet_id      = aws_subnet.nexusiot_public_subnet.id
  route_table_id = aws_route_table.nexusiot_rt.id
}

# 6. Security Group (Virtual Firewall)
resource "aws_security_group" "nexusiot_sg" {
  name        = "nexusiot_sg"
  description = "Allow inbound traffic for NexusIoT services (SSH, MQTT, API, Grafana, Kafka UI)"
  vpc_id      = aws_vpc.nexusiot_vpc.id

  # SSH Access
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # MQTT Broker (For external device telemetry)
  ingress {
    description = "Mosquitto MQTT"
    from_port   = 1883
    to_port     = 1883
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # FastAPI API Gateway
  ingress {
    description = "FastAPI Gateway"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Grafana Dashboard
  ingress {
    description = "Grafana Dashboard"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Kafka UI
  ingress {
    description = "Kafka UI Web Console"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Prometheus Console
  ingress {
    description = "Prometheus UI"
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Egress Rule (Allow all outbound traffic)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "NexusIoT-SecurityGroup"
  }
}

# 7. Get Latest Ubuntu 24.04 LTS AMI
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# 8. EC2 Instance Provisioning
resource "aws_instance" "nexusiot_server" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = var.instance_type
  key_name      = var.key_name
  subnet_id     = aws_subnet.nexusiot_public_subnet.id

  vpc_security_group_ids = [aws_security_group.nexusiot_sg.id]

  # User data script to configure swap memory, Docker, and Kubernetes tools
  user_data = <<-EOF
              #!/bin/bash
              apt-get update -y

              # 1. Setup 4GB of Swap Space (Essential for t2.micro/t3.micro to avoid OOM)
              fallocate -l 4G /swapfile
              chmod 600 /swapfile
              mkswap /swapfile
              swapon /swapfile
              echo '/swapfile none swap sw 0 0' | tee -a /etc/fstab

              # 2. Install Docker
              apt-get install -y ca-certificates curl gnupg
              install -m 0755 -d /etc/apt/keyrings
              curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
              chmod a+r /etc/apt/keyrings/docker.gpg
              echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu noble stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
              apt-get update -y
              apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
              usermod -aG docker ubuntu

              # 3. Install Minikube
              curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
              install minikube-linux-amd64 /usr/local/bin/minikube

              # 4. Install kubectl
              curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
              install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
              EOF

  tags = {
    Name = "NexusIoT-Server"
  }
}
