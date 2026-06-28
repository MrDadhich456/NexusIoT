output "instance_public_ip" {
  description = "The public IP address of the EC2 instance"
  value       = aws_instance.nexusiot_server.public_ip
}

output "ssh_command" {
  description = "Command to SSH into the EC2 instance"
  value       = "ssh -i ${var.key_name}.pem ubuntu@${aws_instance.nexusiot_server.public_ip}"
}

output "api_url" {
  description = "URL to access the FastAPI documentation"
  value       = "http://${aws_instance.nexusiot_server.public_ip}:8000/docs"
}

output "grafana_url" {
  description = "URL to access the Grafana dashboard"
  value       = "http://${aws_instance.nexusiot_server.public_ip}:3000"
}

output "kafka_ui_url" {
  description = "URL to access the Kafka UI console"
  value       = "http://${aws_instance.nexusiot_server.public_ip}:8080"
}

output "prometheus_url" {
  description = "URL to access the Prometheus UI console"
  value       = "http://${aws_instance.nexusiot_server.public_ip}:9090"
}
