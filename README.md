# Monitor train delays

A website to visualize train delays (under development)

![under-development](https://img.shields.io/badge/Status-under%20development-red.svg
) ![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg) ![Website](https://img.shields.io/badge/Website-down-red.svg)
![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)


## Goal

Making use of the Deutsche Bahn's API for timetable and timetable changes (https://developer.deutschebahn.com/store/apis/info?name=Timetables&version=v1&provider=DBOpenData&#/), this app collects and displays delay data of trains departing from a specific train station.

## Tools used

![kafka](https://img.shields.io/badge/kafka-black.svg?&style=for-the-badge&logo=apache%20kafka) ![docker](https://img.shields.io/badge/docker-blue.svg?&style=for-the-badge&logo=docker&logoColor=white) ![spark](https://img.shields.io/badge/spark-orange.svg?&style=for-the-badge&logo=apache%20spark&logoColor=white) ![airflow](https://img.shields.io/badge/airflow-lightblue.svg?&style=for-the-badge&logo=apache%20airflow&logoColor=white) ![streamlit](https://img.shields.io/badge/streamlit-red.svg?&style=for-the-badge&logo=streamlit&logoColor=white)


* Docker-compose to orchestrate the Docker containers needed
* Airflow to manage DAGs (tasks) which are executed in fixed intervals
* Apache Kafka to handle streaming messages from the Deutsche Bahn API
* Apache Spark to work with streaming data
* Streamlit to display train delay data

## How to use

1. Clone this repository and install docker-compose
2. Use `docker-compose up -d` to start the pipeline
3. Go to `localhost:8501` to see collected data over time

Currently, data are collected for the München-Pasing train station. To change this, change the `eva` variable in `functions.py`. The eva (ID) of every other station from Deutsche Bahn can be fetched by means of the `get /station/{pattern}` API (see https://developer.deutschebahn.com/store/apis/info?name=Timetables&version=v1&provider=DBOpenData&#!/default/get_station_pattern).

## License

MIT license