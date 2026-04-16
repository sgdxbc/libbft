# $`\mathtt{lib}\beta\phi\tau`$

The academic codebase for painless BFT researching and evaluation.

[![CI](https://github.com/sgdxbc/libbft/actions/workflows/ci.yml/badge.svg)](https://github.com/sgdxbc/libbft/actions/workflows/ci.yml)

## Highlights

*Perfect observability.*
The codebase is thoroughly instrumented and has distributed tracing and metrics set up.
No more meditation for reasoning about why it stuck or why it runs so slow.

<details>
<summary>Screenshots</summary>  
![Jaeger screenshot](./docs/screenshots/jaeger.png)
![Prometheus throughput screenshot](./docs/screenshots/prometheus-throughput.png)
![Prometheus latency screenshot](./docs/screenshots/prometheus-latency.png)
</details>

*Pure state machine protocols.*
Core protocol logics are completely separated from surrounding I/O framework and are suitable for porperty-based testing.

*Highly incremental development workflow.* 
Start from (property-based) test cases.
Then in-process cluster with in-memory network.
Then multi-process cluster with localhost network.
Then bring your own cluster or AWS credential.
This codebase enables addressing issues with minimum infrastructures that can reproduce them before moving on to more involved setups.

## Quick Start

Coming soon.
