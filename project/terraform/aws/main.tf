provider "aws" {
    access_key = "${var.aws_access_key}"
    secret_key = "${var.aws_secret_key}"
    region = "${var.region}"
}

module "s3" {
    source = "./s3/"
    #bucket name should be unique
    bucket_name = "weather-data-bucket-1"       
    database_name = "db_name_weather"
    master_username = "${var.master_username_dw}"
    master_password = "${var.master_password_dw}"
}


# resource "aws_s3_bucket"  {

#    bucket = "${var.bucket_name}"

#    acl = "private"  

# }

# resource "aws_s3_bucket" "b" {
#   bucket = "${var.bucket_name}"
#   acl    = "private"
#   tags = {
#     Name        = "My bucket"
#     Environment = "Dev"
#   }
# }