variable "clickhouse_password" {
  description = "Clickhouse admin password"
  type        = string
  sensitive   = true
}

output "clickhouse_host_fqdn" {
  value = resource.yandex_mdb_clickhouse_cluster.clickhouse-dev.host[0].fqdn
}