resource "aws_eks_cluster" "this" {
  count    = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  name     = format("%s-%s", var.aws_project, local.app_id)
  version  = "1.36"
  role_arn = local.eks_role_arn

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  vpc_config {
    endpoint_public_access  = true
    endpoint_private_access = true
    subnet_ids              = local.public_subnet_ids
    security_group_ids      = data.aws_security_groups.this.ids
  }

  tags = local.app_tags
}

resource "aws_eks_node_group" "this" {
  count           = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  cluster_name    = one(aws_eks_cluster.this.*.name)
  node_group_name = format("%s-%s", var.aws_project, local.app_id)
  node_role_arn   = local.eks_role_arn
  subnet_ids      = local.public_subnet_ids
  capacity_type   = var.aws_eks_type
  instance_types  = ["t3.medium", "t3a.medium", "t2.medium"]
  disk_size       = 50

  scaling_config {
    desired_size = 1
    max_size     = 1
    min_size     = 1
  }

  update_config {
    max_unavailable = 1
  }

  tags = local.app_tags
}

resource "null_resource" "helm_chart" {
  count = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0

  triggers = {
    cluster_id  = one(aws_eks_node_group.this.*.id)
    source_hash = local.helm_hash
    aws_ds_ip   = var.aws_ds_ip
  }

  provisioner "local-exec" {
    command = <<-EOT
      if [ -z "${var.aws_ds_ip}" ] || [ "${var.aws_ds_ip}" = "null" ]; then echo "ERROR: aws_ds_ip is not set"; exit 1; fi && \
      aws eks update-kubeconfig --region ${data.aws_region.this.region} --name ${one(aws_eks_cluster.this.*.name)} || true && \
      helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ && helm repo update && \
      helm upgrade --cleanup-on-fail \
        --install jupyterhub jupyterhub/jupyterhub \
        --set proxy.secretToken="${sha256(random_pet.this.id)}" \
        --set hub.config.LDAPAuthenticator.server_address="${var.aws_ds_ip}" \
        --set-string "hub.config.LDAPAuthenticator.allowed_groups[0]=CN=${local.app_id}\,OU=Users\,OU=CORP\,DC=corp\,DC=codingworkshop\,DC=net" \
        --namespace default --values ./helm/config.yaml
    EOT
  }
}

# Settings for the data engineering jobs (data/*): where the sources and the
# lake are, and how to reach Aurora. The password goes into a Kubernetes Secret
# (pythonJob.secretEnv), everything else into plain environment variables.
locals {
  python_job_env = {
    IS_LOCAL           = local.env_vars.IS_LOCAL
    AWS_REGION         = data.aws_region.this.region
    AWS_DEFAULT_REGION = data.aws_region.this.region
    POSTGRES_HOST      = local.env_vars.POSTGRES_HOST
    POSTGRES_PORT      = tostring(local.env_vars.POSTGRES_PORT)
    POSTGRES_NAME      = local.env_vars.POSTGRES_NAME
    POSTGRES_USER      = local.env_vars.POSTGRES_USER
    DATA_LAKE_BUCKET   = aws_s3_bucket.datalake.id
    LAKE_ROOT          = format("s3://%s/lake", aws_s3_bucket.datalake.id)
    EVENTS_SOURCE_URI  = format("s3://%s/source/learning_activity", aws_s3_bucket.datalake.id)
    SEED_SOURCE_URI    = format("s3://%s/source/postgres_seed", aws_s3_bucket.datalake.id)
  }
}

resource "local_sensitive_file" "python_job_values" {
  for_each        = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? local.data_names_python : {}
  filename        = format("%s/.terraform/helm-values-%s.yaml", path.module, each.key)
  file_permission = "0600"
  content = yamlencode({
    pythonJob = {
      env       = local.python_job_env
      secretEnv = { POSTGRES_PASS = local.env_vars.POSTGRES_PASS }
    }
  })
}

resource "null_resource" "helm_python_job" {
  for_each = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? local.data_names_python : {}

  triggers = {
    cluster_id  = one(aws_eks_node_group.this.*.id)
    source_hash = timestamp()
  }

  provisioner "local-exec" {
    command = <<-EOT
      helm upgrade --cleanup-on-fail \
        --install ${replace(each.key, "_", "-")}-python-job ./helm/ \
        --set pythonJob.enabled=true \
        --set pythonJob.name=${replace(each.key, "_", "-")} \
        --set-file pythonJob.scriptContent=${each.value.path}/${each.value.file} \
        --set-file pythonJob.requirementsContent=${each.value.path}/${each.value.reqs} \
        --values ${local_sensitive_file.python_job_values[each.key].filename} \
        --namespace default
    EOT
  }

  depends_on = [null_resource.helm_chart, null_resource.datalake_sources]
}
