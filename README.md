# Vehicle Data Handling Solution

## Overview

This solution integrates client-side vehicle simulation with a server-side data processing architecture to collect, stream, and store vehicular diagnostics data (OBD2). Designed to simulate real-world traffic scenarios and analyze vehicle performance, it comprises two main components:

### Client Side

- Utilizes **SUMO** (Simulation of Urban MObility) for simulating vehicles that generate OBD2 data.
- Simulated vehicles send data over **Apache Kafka**, ensuring robust data streaming.
- Targeted to run on **Windows machines**, facilitating ease of deployment.

### Server Side

- **Apache Kafka Server** acts as the central point for receiving OBD2 data streams from numerous clients.
- **GreenPlum Database**, an MPP SQL database built on PostgreSQL, stores the ingested data, allowing for advanced data analytics.
- The server components are designed to run on a **RHEL 9 VM**, providing a stable and secure environment for data processing and storage.

## Conclusion

By combining SUMO's traffic simulation capabilities with the robust data streaming and storage provided by Apache Kafka and GreenPlum Database, all running on a RHEL 9 VM, this framework offers a scalable and flexible solution for real-time vehicular data analysis.

# Steps to Run the Solution

Follow these steps to get the vehicle data handling solution up and running, from initiating the servers to executing the client simulations.

## Starting the Servers

1. **Start Kafka and GreenPlum Server**:
   Navigate to the server folder and execute the bash script provided. This script initiates both the Kafka and GreenPlum servers, setting up the necessary environment for data streaming and storage.
   ```bash
   cd server
   ./run_all_servers.sh

2. **Run fleet_server_run.py**
3. **Run clients_run.py**


# Development Environment Setup Guide

Welcome to the comprehensive setup guide for your development environment on Red Hat Enterprise Linux (RHEL) 9. This guide covers the installation of Visual Studio Code, Apache Kafka, GreenPlum Database, and the required Python packages to get you started.

## Table of Contents

- [Visual Studio Code](#visual-studio-code)
- [Apache Kafka](#apache-kafka)
- [GreenPlum Database](#greenplum-database)
- [Python Setup](#python-setup)
- [Running GreenPlum Database](#running-greenplum-database)

---

## Visual Studio Code

Visual Studio Code (VS Code) is a free, open-source editor that supports debugging, embedded Git control, syntax highlighting, intelligent code completion, snippets, and code refactoring.

### Installation Steps:

1. **Import Microsoft GPG Key**:
    ```bash
    sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc
    ```

2. **Add VS Code Repository**:
    ```bash
    sudo sh -c 'echo -e "[code]\nname=Visual Studio Code\nbaseurl=https://packages.microsoft.com/yumrepos/vscode\nenabled=1\ngpgcheck=1\ngpgkey=https://packages.microsoft.com/keys/microsoft.asc" > /etc/yum.repos.d/vscode.repo'
    ```

3. **Install VS Code**:
    ```bash
    dnf check-update
    sudo dnf install code # or code-insiders
    ```

## Apache Kafka

Apache Kafka is a distributed streaming platform that lets you publish and subscribe to streams of records, store streams of records in a fault-tolerant way, and process streams of records as they occur.

### Installation and Configuration:

1. **Install Kafka Dependencies**:
    ```bash
    sudo dnf install python3-pip
    ```

2. **Configure Server Properties**:
    - Obtain your IP address using `ip addr show`.
    - Update the `server.properties` file with your IP address:
        ```properties
        listeners=PLAINTEXT://0.0.0.0:9092
        advertised.listeners=PLAINTEXT://your.vm.ip.address:9092
        ```

3. **System Configurations**:
    ```bash
    sudo dnf install dbus-x11
    eval $(dbus-launch --sh-syntax)
    sudo firewall-cmd --zone=public --add-port=5432/tcp --permanent
    sudo firewall-cmd --reload
    sudo setenforce 0
    ```

4. **Running Kafka**:
    Refer to the [official Kafka documentation](https://linuxtldr.com/installing-apache-kafka/?utm_content=cmp-true#google_vignette) for running Kafka using:
    - `zookeeper-server-start.sh $HOME/kafka/config/zookeeper.properties`
    - `kafka-server-start.sh $HOME/kafka/config/server.properties`

## GreenPlum Database

GreenPlum Database is an MPP SQL database based on PostgreSQL and is designed for large-scale data warehousing and analytics.

### Setup and Initialization:

1. **Configuration Files**:
    ```bash
    mkdir gpconfigs
    cp /usr/local/greenplum-db-7.1.0/docs/cli_help/gpconfigs/gpinitsystem_config ~/gpconfigs/my_gpinitsystem_config
    ```

2. **Modify `my_gpinitsystem_config`** as per your setup requirements.

3. **Prepare System and Database**:
    Follow the detailed steps provided in the previous instructions for setting up your GreenPlum environment, including creating directories, generating SSH keys, and modifying host files.

4. **Initialize Database**:
    ```bash
    gpinitsystem -c ~/gpconfigs/my_gpinitsystem_config -h ~/gpconfigs/hostfile.txt
    ```

5. **Environment Configuration**:
    ```bash
    source /usr/local/greenplum-db-7.1.0/greenplum_path.sh
    export COORDINATOR_DATA_DIRECTORY=/data/coordinator/gpseg-1
    ```

6. **Verify Installation**:
    ```bash
    gpstate -d /data/coordinator/gpseg-1
    ```

## Python Setup

Ensure you have the necessary Python packages installed for your projects.

### Install psycopg2-binary:

```bash
pip install psycopg2-binary
```

## running-greenplum-database

```bash
source /usr/local/greenplum-db-7.1.0/greenplum_path.sh
export COORDINATOR_DATA_DIRECTORY=/data/coordinator/gpseg-1
gpstart
```


## Install Redis

Follow these commands to install Redis on your system:

```bash
sudo dnf update
sudo dnf install redis
sudo systemctl start redis
sudo systemctl enable redis
```

## How to set psw
```bash
redis-cli
set mykey somevalue
get mykey
```

## How to install WebSockets
```bash
pip3 install websockets
pip install aioredis
```

## How to open ports in the firewall
```bash
sudo firewall-cmd --zone=public --add-port=6789/tcp --permanent
sudo firewall-cmd --reload


sudo firewall-cmd --zone=public --add-port=8765/tcp --permanent
sudo firewall-cmd --reload
```

