# Big Data Project

### Required

* Python 3.9.8
* Java 8+
* Pyspark 3.2.0
* Dataset: [NYC Taxi Fare](https://archive.org/details/nycTaxiTripData2013)

### To setup project:

1. Install required version of interpreter/libraries
2. VCS:
   - Clone repo
   - Create your own branch from master (e.x. yuriy_gonsor_branch)
   - Specify paths to your datasets in [config.py](config.py)
3. Docker
   - Install Docker
   - Build Docker image with command (run this command after the code changes)

```shell
docker-compose up -d --no-deps --build
```

* * Start container and watch logs in terminal

```shell
docker start sparkapp-sparkapp-1 -i
```

* To delete unused images

```shell
docker image prune -f
```

* P.S. it is better for you to run these commands from README

