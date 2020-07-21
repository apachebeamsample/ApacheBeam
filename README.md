# ApacheBeam

# Apache Flink and running locally

This is a set up so that we can easily create a Flink runner
 (and take advantage of the Flink Runtime admin UI)
  and React (set up with [`Running Apache Flink`](https://ci.apache.org/projects/flink/flink-docs-release-1.11/try-flink/local_installation.html)) 
  
## Running

1. `docker-compose build`
1. `docker-compose up`
1. There should now be two servers running:
  - [http://127.0.0.1:8081](http://127.0.0.1:8081) is the Admin interface for Flink
  - [http://127.0.0.1:6123](http://127.0.0.1:6123) is the TaskManager 