# Sending OpenConfig Telemetry data to Kafka

[![Apache 2.0 License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

The objective is to publish OpenConfig Telemetry interface stats to a Kafka BUS. We will leverage different Open Source projects:

- [Go](https://github.com/golang/go) as the main programming language.
- [xrgrpc](https://nleiva.github.io/xrgrpc/) (gRPC library for Cisco IOS XR) to interact with IOS XR via [gRPC](https://grpc.io/).
- [ygot](https://github.com/openconfig/ygot) (**Y**ANG **Go** **T**ools) to generate the [OpenConfig](http://www.openconfig.net/) data structures required for the gRPC calls.
- [sarama](https://github.com/Shopify/sarama) an MIT-licensed Go client library for Apache Kafka.

## Examples

### Monitor Interface Stats

The objective is to publish OpenConfig Telemetry interface stats to a Kafka BUS.

__Steps__
1. Configure a Streaming Telemetry subscription using [ygot](https://github.com/openconfig/ygot) and [xrgrpc](https://nleiva.github.io/xrgrpc/).
2. Subscribes to a Telemetry stream to learn interfaces stats.
3. Send a summary JSON output to a Kafka BUS using [sarama](https://github.com/Shopify/sarama).

## Tutorials
- [Programming IOS-XR with gRPC and Go](https://xrdocs.github.io/programmability/tutorials/2017-08-04-programming-ios-xr-with-grpc-and-go/).
- [Validate the intent of network config changes](https://xrdocs.github.io/programmability/tutorials/2017-08-14-validate-the-intent-of-network-config-changes/).
