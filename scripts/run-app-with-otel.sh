#!/bin/bash -xe
OTEL_TRACES_SAMPLER=traceidratio OTEL_TRACES_SAMPLER_ARG=0.001 RUST_LOG=info,libbft=warn cargo run -r --bin $@