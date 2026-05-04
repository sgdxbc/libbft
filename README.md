<div align="center">
<img src="./docs/logo.svg" width=160>

# &#x1d695;&#x1d692;&#x1d68b;&#x1d4d1;&#x1d4d5;&#x1d4e3;

The academic codebase for painless BFT researching and evaluation.

[![CI](https://github.com/sgdxbc/libbft/actions/workflows/ci.yml/badge.svg)](https://github.com/sgdxbc/libbft/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-AGPL--3.0-orange.svg)
![Code lines](./docs/loc-badge.svg)
</div>

You will find &#x1d695;&#x1d692;&#x1d68b;&#x1d4d1;&#x1d4d5;&#x1d4e3; useful for
- Developing novel Byzantine fault tolerance protocols or improving on existing ones.
  The codebase offers all kinds of life-saving scaffolding and standard implementations of seminar protocols, letting you focus on innovation and establish apple-to-apple comparison with least efforts.
- Developing academic distributed systems that would make use of BFT protocols are building blocks.
  The protocols are not your contribution and you don't bother to implement by yourself, while the existing implementations do not fit in your codebase and you don't want to bet your evaluation on vibe coding.

## Highlights

**Perfect observability.**
The codebase is thoroughly instrumented and has distributed tracing and metrics set up.
No more meditation for reasoning about *where it stuck* or *why it runs so slow*.

<details>
<summary>Screenshots</summary>  

![Jaeger screenshot](./docs/screenshots/jaeger.png)
![Prometheus throughput screenshot](./docs/screenshots/prometheus-throughput.png)
![Prometheus latency screenshot](./docs/screenshots/prometheus-latency.png)
</details>

**Streamlined modular implementations.**
Everything in the codebase follows actor model with a regular and comprehensive set of interfaces.
Core protocol state machines are completely decoupled with peripheral I/O framework and are suitable for property-based testing, i.e., [sans I/O].
Both layers of abstraction are self-contained and portable: you can easily plug the single module into your other awesome projects without depending on the whole crate.

[sans I/O]: https://sans-io.readthedocs.io/

**Highly incremental development workflow.**
Start from (property-based) test cases.
Then in-process cluster with in-memory network.
Then multi-process cluster with localhost network.
Then bring your own cluster or AWS credential.
This codebase enables addressing issues with minimum infrastructures that can reproduce them before moving on to more involved setups.

**Performant and simple protocol code written in Rust.**
You probably cannot get better performance from any implementation with this amount of code.

## Quick Start

> Coming soon.
