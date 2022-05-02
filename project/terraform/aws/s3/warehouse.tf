resource "aws_redshift_cluster" "data_logs_db" {
  cluster_identifier = "data-logs-cluster"
  database_name      = "${var.database_name}" 
  master_username    = "${var.master_username}"
  master_password    = "${var.master_password}"
  node_type          = "dc1.large"
  cluster_type       = "single-node"
}