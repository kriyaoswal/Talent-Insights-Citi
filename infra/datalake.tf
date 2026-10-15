# Data lake for the data engineering jobs (data/*, run on EKS).
#
#   s3://<project>-datalake-<app_id>/
#     source/learning_activity/...   "S3 Storage" data source (raw JSONL events)
#     source/postgres_seed/...       sample CSVs + schema.sql, loaded into an empty Aurora by the job
#     lake/bronze|silver|gold|_runs  the Medallion layers written by the job
#
# Created in AWS and in LocalStack. The sample data is uploaded on deploy
# (only when it changes) so the job finds its sources on its first run.
# The bucket and its paths reach the job through local.python_job_env (eks.tf).

locals {
  datalake_bucket = format("%s-datalake-%s", var.aws_project, local.app_id)
  sample_data_dir = abspath(format(
    "%s/../sampleData/data-engineering-sample-performance-scenario/data-engineering-performance-talent-management",
    path.module
  ))
  sample_data_files = sort(concat(
    tolist(fileset(local.sample_data_dir, "learning_activity/**/*.jsonl")),
    tolist(fileset(local.sample_data_dir, "*.csv")),
    tolist(fileset(local.sample_data_dir, "schema.sql")),
  ))
  sample_data_hash = sha256(join("", [
    for f in local.sample_data_files : format("%s:%s", f, filesha256(format("%s/%s", local.sample_data_dir, f)))
  ]))
}

resource "aws_s3_bucket" "datalake" {
  bucket        = local.datalake_bucket
  force_destroy = true

  lifecycle {
    prevent_destroy = false
  }

  tags = local.app_tags
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "null_resource" "datalake_sources" {
  count = length(local.sample_data_files) > 0 ? 1 : 0

  triggers = {
    bucket    = aws_s3_bucket.datalake.id
    data_hash = local.sample_data_hash
  }

  provisioner "local-exec" {
    command = <<-EOT
      aws s3 sync "${local.sample_data_dir}/learning_activity" \
        "s3://${aws_s3_bucket.datalake.id}/source/learning_activity" --only-show-errors && \
      aws s3 cp "${local.sample_data_dir}" "s3://${aws_s3_bucket.datalake.id}/source/postgres_seed" \
        --recursive --exclude "*" --include "*.csv" --include "schema.sql" --only-show-errors
    EOT
  }
}
