
# slowly-changing-dimension

This is the Docker configuration which allows you to run Apache Spark without installing any dependencies on your machine!<br/>
OK, any except `docker`.

## Prerequisites

As stated, the thing you need is `docker`.

Follow the instructions on [Install Docker](https://docs.docker.com/engine/installation/) for your environment if you haven't got `docker` already.

## Usage

### Prepare the image

Switch to `docker` directory here and run `docker build -t pyspark:scd .` (don't forget the final `.`) to build your docker image. That may take some time but is only required once. Or perhaps a few times after you tweak something in a `Dockerfile`.

After the process is finished you have a `pyspark:scd` image, that will be the base for your experiments. You can confirm that looking on results of `docker images` command.

### Run the container

From your project folder, run `docker run -it -v $(pwd):/scd/ pyspark:scd /bin/bash`
This will start the container and open up a bash console inside it.
At this point you can execute `python`, a pyspark console with `pyspark` or run test cases using `pytest`
