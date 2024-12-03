# Bike Sharing Monitoring using Kafka

This repository provides a real-time monitoring system for Bike Sharing Systems using Kafka. It connects to the public JCDecaux API to gather bike station data, processes the data, and sends alerts for empty stations.

## Steps to Get Started

First clone the repository and insert [your private API key](https://developer.jcdecaux.com/#/signup) in the `config/private_config.py` file, following the template `config/template_private_config.py`.

Install all necessary dependencies by running:
```bash
   pip install -r requirements.txt
```

Start Zookeeper and Kafka:
```bash
   # On Linux
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties

   # On Windows
   .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
   .\bin\windows\kafka-server-start.bat .\config\server.properties
```

After starting Zookeeper and Kafka, Windows users might need to set the Python Path
```bash
   $env:PYTHONPATH="<your_project_path>"
```

## Usage

Run the scripts in separate terminals to start the data streaming pipeline:
```bash
   python scripts/ingest-data.py
   python scripts/stations-activity.py
   python scripts/empty-stations.py
   python scripts/alert-empty-stations.py
   python scripts/archive-data.py
   python scripts/monitor-kafka.py
```