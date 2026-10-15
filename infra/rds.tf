resource "aws_db_subnet_group" "this" {
  count      = data.aws_caller_identity.this.id != "000000000000" && var.aws_postgres_enabled ? 1 : 0
  name       = format("%s-rds-subnet-group-%s", var.aws_project, local.app_id)
  subnet_ids = local.public_subnet_ids
  tags       = local.app_tags
}

resource "aws_rds_cluster" "this" {
  count                   = data.aws_caller_identity.this.id != "000000000000" && var.aws_postgres_enabled ? 1 : 0
  cluster_identifier      = format("%s-rds-%s", var.aws_project, local.app_id)
  engine                  = "aurora-postgresql"
  engine_mode             = "provisioned"
  engine_version          = "17.7"
  master_username         = "superadmin"
  master_password         = random_pet.this.id
  database_name           = replace(var.aws_project, "-", "")
  backup_retention_period = 7
  preferred_backup_window = "07:00-09:00"
  skip_final_snapshot     = true
  storage_encrypted       = true
  db_subnet_group_name    = element(aws_db_subnet_group.this.*.name, count.index)
  # Plus the EKS cluster security group (when EKS is enabled): EKS nodes are in that group and it
  # allows traffic between its members, so data jobs can reach Aurora on 5432.
  vpc_security_group_ids = concat(
    data.aws_security_groups.this.ids,
    [for c in aws_eks_cluster.this : c.vpc_config[0].cluster_security_group_id],
  )
  enabled_cloudwatch_logs_exports = ["postgresql"]

  serverlessv2_scaling_configuration {
    max_capacity = 4.0
    min_capacity = 0.0
  }

  tags = local.app_tags

  # Added by AWS monitoring outside Terraform; don't remove it on every deploy.
  lifecycle {
    ignore_changes = [tags["ams:rt:ams-monitoring-policy"], tags_all["ams:rt:ams-monitoring-policy"]]
  }
}

resource "aws_rds_cluster_instance" "this" {
  count                      = data.aws_caller_identity.this.id != "000000000000" && var.aws_postgres_enabled ? 1 : 0
  cluster_identifier         = element(aws_rds_cluster.this.*.id, count.index)
  engine                     = element(aws_rds_cluster.this.*.engine, count.index)
  engine_version             = element(aws_rds_cluster.this.*.engine_version, count.index)
  identifier                 = format("%s-rds-%s", var.aws_project, local.app_id)
  instance_class             = "db.serverless"
  auto_minor_version_upgrade = true
  tags                       = local.app_tags

  # Added by AWS monitoring outside Terraform; don't remove it on every deploy.
  lifecycle {
    ignore_changes = [tags["ams:rt:ams-monitoring-policy"], tags_all["ams:rt:ams-monitoring-policy"]]
  }
}
