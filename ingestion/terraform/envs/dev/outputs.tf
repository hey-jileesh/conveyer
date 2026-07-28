# LLD S10.1: "Root outputs re-export platform outputs plus per-feed
# function names."

output "platform" {
  description = "Passthrough of the platform module's full output object (LLD S10.6)."
  value       = module.platform
}

output "feed_driver_function_names" {
  description = "Map of feed_id -> per-feed sftp-pull driver Lambda function name. s3-push feeds are omitted (they have no per-feed compute; they're served by the shared registrar, part of the platform outputs above)."
  value = {
    for feed_id, mod in module.feed : feed_id => mod.driver_function_name
    if mod.driver_function_name != null
  }
}
