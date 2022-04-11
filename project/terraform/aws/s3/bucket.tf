resource "aws_s3_bucket" "demo2" {
    bucket = "${var.bucket_name}" 
    acl = "${var.acl_value}"   
}