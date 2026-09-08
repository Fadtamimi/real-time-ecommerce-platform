variable "project_name" {
  description = "Short, lowercase name used to label AWS resources."
  type        = string
  default     = "ecommerce-platform"
}

variable "region" {
  description = "AWS region for the deployment. Matches the AWS Console region."
  type        = string
  default     = "eu-north-1"
}

variable "enable_schedule" {
  description = "Enable the daily EventBridge schedule after verifying one manual Glue run."
  type        = bool
  default     = false
}

variable "monthly_budget_limit_usd" {
  description = "Monthly AWS budget guardrail, shown in Billing."
  type        = number
  default     = 20
}

variable "enable_msk" {
  description = "Create the MSK Serverless cluster. This is the highest-cost component."
  type        = bool
  default     = false
}
