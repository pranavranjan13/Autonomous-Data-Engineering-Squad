# Autonomous Multi-Agent Data Engineering Squad

## Executive Summary

An end-to-end autonomous engineering system designed to architect, validate, and deploy production-grade data pipelines. This project automates the Senior Data Engineer's role in enforcing governance for high-scale (500GB+) data workloads.

## The Problem

Manual code reviews for large-scale data pipelines are time-intensive and prone to governance skips, such as missing partitioning or inferred schemas, which lead to compute-cost overruns in production.

## The Solution (Agentic Architecture)

I built a tri-agent system using **Microsoft AutoGen (AG2)** and the **Euri AI Gateway** that follows a strict DataOps lifecycle:

1. **The Data Architect (Agent):** Generates PySpark logic based on enterprise partitioning rules.
2. **Deterministic Validator (Logic):** A local Python-based quality gate that enforces strict syntax rules (Partitioning, explicit schemas, to_date derivation).
3. **The Cloud Architect (Agent):** Translates approved logic into Multi-Cloud IaC (Terraform for AWS Glue and YAML for Azure Databricks).

## Key Technical Achievements

- **Cost-Aware AI:** Engineered a sliding context window to manage a 200k daily token limit, ensuring project sustainability.
- **Self-Healing Loop:** Implemented a feedback mechanism where agents rewrite code based on failed validation cycles.
- **Model Agnostic:** Used a custom Model Client bridge to swap between high-performance models (Qwen/Gemini) seamlessly.

## Tech Stack

- **Frameworks:** Microsoft AutoGen (AG2), Pydantic
- **Languages:** Python (3.10+), PySpark, Terraform
- **Cloud:** AWS (Glue), Azure (Databricks)
- **AI Gateway:** Euron API (Gemini 2.5 Flash, Qwen-2.5-Coder)
