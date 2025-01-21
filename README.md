# Bento - 0.1.0

Bento is an indexing solution for [Kadena](https://kadena.io) blockchain written in Rust, with advanced analytics capabilities for understanding user behavior and token movements.

[![Build Status:](https://github.com/ThinEdgeLabs/bento/workflows/CI%20Tests/badge.svg)](https://github.com/ThinEdgeLabs/bento/actions?query=workflow%3A%22CI+Tests%22+branch%3Amain)

## Features
* Out-of-the-box indexes blocks, transactions, events and token transfers
* Automatically removes orphan blocks
* Handles missed blocks
* HTTP API
* Advanced analytics capabilities:
  - Token holding period analysis
  - Transaction frequency metrics
  - Wallet relationship mapping
  - Real user behavior differentiation
  - Interactive visualization through Metabase

## Coming soon
* An easy way to extend it and index any custom modules (eg. Marmalade)
* Enhanced analytics features:
  - Advanced wallet clustering
  - Token flow visualization
  - User behavior pattern detection
  - Custom metrics builder

## Setup

### Prerequisites

* [Chainweb Node](https://github.com/kadena-io/chainweb-node)
* [Docker] (optional)
* Metabase (for analytics visualization)

#### Using Docker

The fastest way is to use Docker / Docker Compose as the repo already comes with a docker-compose configuration file.
Alternatively you would need to install PostgreSQL, [rust and cargo to build](Build with Cargo) and then execute the binaries.

**Important**: the `headerStream` chainweb node config needs to be set to `true`. You can check the [configuring the node](https://github.com/kadena-io/chainweb-data#configuring-the-node) section of `chainweb-data` for more details.

Once the node is setup and synced you can continue with the installation:

1. Clone the repository:
git clone git@github.com:YourUsername/bento.git
Copy2. Create a `.env` file, check the `.env-example` to see how it should look like.
3. Start the containers:
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
Copy
[Previous installation instructions remain the same...]

## Analytics Features

Bento now includes powerful analytics capabilities designed to help you understand real user behavior:

### Token Analysis
* Track token holding periods
* Monitor token transfer patterns
* Analyze token distribution

### User Behavior
* Distinguish between real users and automated accounts
* Track wallet interaction patterns
* Measure user engagement metrics

### Wallet Analysis
* Map wallet relationships
* Track transaction frequencies
* Identify connected wallet clusters

### Using Analytics

Access analytics through:

1. Direct API endpoints:
```bash
# Get token holding periods
GET /analytics/holding-period/{token_id}

# Get wallet connections
GET /analytics/wallet-connections/{address}

# Get transaction frequency
GET /analytics/tx-frequency/{address}

Metabase Dashboard:


Access at http://your-host:3001
Pre-built dashboards for common metrics
Create custom visualizations

[Previous API documentation remains the same...]
Development
[Previous development instructions remain the same...]
Analytics Development
To extend the analytics capabilities:

Add new metrics in src/indexer/analytics.rs
Create corresponding API endpoints in src/api/routes/analytics.rs
Add Metabase visualizations for new metrics

[Previous contributing guidelines remain the same...]
License
[Previous license information remains the same...]