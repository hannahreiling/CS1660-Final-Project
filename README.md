# Hannah Reiling (her46) CS1660 Final Project

Contents of repository:
- UserInterface.java
	- Communicates with user and displays input and output; built as part of Maven project and deployed on Docker container
- InvertedIndex.java
	- Contains the algorithm to construct an Inverted Index; deployed as a JAR on GCP cluster

Video link (containing execution and code walkthrough): https://vimeo.com/406983470 (new video link coming soon with hopefully better quality)

Items completed from "Project Grading Criteria" slide (no extra credit done):
- First Java Application Implementation and Execution on Docker
- Docker to Local (or GCP) Cluster Communication
- Inverted Indexing MapReduce Implementation and Execution on the Cluster (GCP)

Docker repository with latest image: https://hub.docker.com/repository/docker/her46/cs1660-final-project

Execution steps (on Mac terminal):
- Instructions for running GUIs with Docker (https://cntnr.io/running-guis-with-docker-on-mac-os-x-a14df6a76efc)
- On terminal run: 
  - open -a Xquartz
  - export DISPLAY=:0.0
  - xhost +
  - docker run -e DISPLAY=(IP addr):0 her46/cs1660-final-project:latest
