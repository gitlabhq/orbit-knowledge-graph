variable "tier" {
  description = "Tier name from bench/config/tiers.yaml (small, medium, large)."
  type        = string
  default     = "small"

  validation {
    condition     = contains(["small", "medium", "large"], var.tier)
    error_message = "tier must be one of: small, medium, large"
  }
}

variable "cluster_name" {
  description = "Override the cluster name. Defaults to ra-bench-{tier}."
  type        = string
  default     = ""
}

variable "dedicated_ch_pool" {
  description = "Create a tainted node pool for the standalone ClickHouse StatefulSet."
  type        = bool
  default     = false
}
