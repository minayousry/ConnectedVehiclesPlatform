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
