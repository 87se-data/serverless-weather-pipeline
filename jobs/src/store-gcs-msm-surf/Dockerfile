FROM almalinux:latest

COPY ./requirements.txt ./

RUN dnf update dnf -y && \
    dnf install -y python3 python3-devel && \
    dnf install -y gcc-c++ glibc-langpack-ja langpacks-ja

RUN python3 -m pip install wheel==0.41.2 setuptools
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY ./main.py ./
COPY ./store-gcs-msm-surf_config.json ./
RUN chmod +x ./main.py
COPY ./grib2.pyx ./
RUN chmod +x ./grib2.pyx
COPY ./setup.py ./
RUN chmod +x ./setup.py

RUN python3 setup.py build_ext --inplace


ENTRYPOINT ["python3", "./main.py"]
CMD ["0"]