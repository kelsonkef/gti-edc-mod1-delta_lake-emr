variable "aws_region" {
  default = "us-east-2"
}

variable "base_bucket_name" {
  default = "datalake-kelson-igti-edc-tf"
}

variable "ambiente" {
  default = "producao"
}

variable "numero_conta" {
  default = "806319895900"
}

variable "lambda_function_name" {
  default = "IGTIexecutaEMR"
}

variable "key_pair_name" {
  default = "minha_key_pair"
}
