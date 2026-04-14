#!/bin/bash -xe
OTEL_SDK_DISABLED=true RUST_LOG=info,libbft=warn cargo run -r --bin $@