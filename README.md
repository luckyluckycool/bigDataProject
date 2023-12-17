# Big Data Project

### Required

* Python 3.9.8
* Java 8+
* Pyspark 3.2.0
* Dataset: [NYC Taxi Fare](https://archive.org/details/nycTaxiTripData2013)

### To setup project:

1. Install required version of interpreter/libraries
2. VCS:
    1. Clone repo
    2. Create your own branch from master (e.x. yuriy_gonsor_branch)
3. Docker
   * Install Docker
   * Build Docker image from Dockerfile and run container with command
```shell
docker build -t sparkapp . | docker run --name sparkapp sparkapp
```

* After running, delete the container and image with command
```shell
docker rm sparkapp | docker rmi sparkapp
```

* Run these command every time you changed code (you can do it from this README)

