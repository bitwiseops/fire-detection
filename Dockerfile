FROM apache/airflow:latest-python3.6
USER root

RUN sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys bca43417c3b485dd128ec6d4b7b3b788a8d3785c

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
        build-essential \
        libgfortran5 \
        locales \
        git \
        vim \
        wget \
        zip \
        unzip \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
RUN wget https://download.esa.int/step/snap/8.0/installers/esa-snap_sentinel_unix_8_0.sh -O esa-snap_sentinel.sh
RUN chmod +x esa-snap_sentinel.sh
RUN ./esa-snap_sentinel.sh -q
ENV PATH "/opt/snap/bin:${PATH}"
RUN /opt/snap/bin/snappy-conf $(which python) /home/airflow/snappy/
ENV PYTHONPATH=$PYTHONPATH:/home/airflow/snappy
RUN sed -i 's+http://cgiar-csi-srtm.openterrain.org.s3.amazonaws.com/source/+https://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/+' /opt/snap/etc/snap.auxdata.properties
USER airflow
RUN pip install numpy rasterio geopandas pyproj shapely scipy scikit-learn boto3
