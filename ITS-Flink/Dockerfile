FROM flink:1.20-java17

# Install Python
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    pip3 install --no-cache-dir \
            python-dotenv==1.1.1 \
            setuptools==77.0.1 && \
    rm -rf /var/lib/apt/lists/*

# Set python3 as default
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip install ruamel.yaml

# Copy extra JARs into Flink's lib so they're loaded automatically
COPY lib/ /opt/flink/lib/

# Copy job sources
COPY src/ /opt/flink/usrlib/

WORKDIR /opt/flink