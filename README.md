# Big Data Project

### Required

* Python 3.9.8
* Java 8+
* Pyspark 3.3.4 (Docker - 3.3.2)
* Dataset: [NYC Taxi Fare](https://archive.org/details/nycTaxiTripData2013)

### To setup project:

1. Install required version of interpreter/libraries
2. VCS:
   - Clone repo
   - Create your own branch from master (e.x. yuriy_gonsor_branch)
   - Specify paths to your datasets in [config.py](config.py)
3. Local Build
   - Install PySpark to your local machine. Hope these tutorials will help you.
     - [PySpark on Windows](https://www.datacamp.com/tutorial/installation-of-pyspark)
     - [PySpark/Jupyter](https://medium.com/@uzzaman.ahmed/setup-pyspark-on-local-machine-and-link-it-with-jupyter-notebook-windows-e08349e792db)
4. Docker - **Alternative way**
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

* To shutdown the docker run next command (it will delete everything)
```shell
docker-compose down
```

* P.S. it is better for you to run these commands from README

