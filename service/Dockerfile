# syndicate-ug-http-svc
#
# VERSION	1.0

#   Copyright 2016 The Trustees of University of Arizona
#
#   Licensed under the Apache License, Version 2.0 (the "License" );
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


FROM	ubuntu:14.04
MAINTAINER	Illyoung Choi <iychoi@email.arizona.edu>

##############################################
# Setup environment
##############################################
ENV LD_LIBRARY_PATH /usr/local/lib

##############################################
# Setup a Syndicate account
##############################################
ENV USER syndicate
ENV HOME /home/$USER

RUN useradd $USER && echo 'syndicate:syndicate' | chpasswd && \
    echo "syndicate ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers && \
    mkdir /home/$USER && \
    chown -R $USER:$USER $HOME

WORKDIR $HOME

##############################################
# Setup Syndicate
##############################################
ENV APT_KEY butler_opencloud_cs_arizona_edu_pub.gpg

# Prep apt to be able to download over https
RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --allow-unauthenticated --no-install-recommends \
    apt-transport-https ca-certificates \
    wget curl unzip build-essential && \
    apt-get clean autoclean && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt /var/lib/cache /var/lib/log

# Copy over and trust https cert
COPY butler.crt /usr/local/share/ca-certificates
COPY $APT_KEY /tmp/

RUN update-ca-certificates && \
    apt-key add /tmp/$APT_KEY && \
    echo "deb https://butler.opencloud.cs.arizona.edu/repos/release/syndicate syndicate main" > /etc/apt/sources.list.d/butler.list

# Install Syndicate
RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --allow-unauthenticated --no-install-recommends \
    syndicate-core && \
    apt-get clean autoclean && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt /var/lib/cache /var/lib/log


##############################################
# Setup Syndicate node.js
##############################################
RUN curl -sL https://deb.nodesource.com/setup_6.x | bash -
RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --allow-unauthenticated --no-install-recommends \
    nodejs && \
    apt-get clean autoclean && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt /var/lib/cache /var/lib/log

USER $USER

# Install syndicate-node
RUN npm install syndicate-storage

# Install syndicate-ug-http
RUN wget -O syndicate-ug-http.zip https://github.com/syndicate-storage/syndicate-ug-http/archive/master.zip && \
    unzip syndicate-ug-http.zip && \
    mv syndicate-ug-http-master syndicate-ug-http && \
    rm syndicate-ug-http.zip

ENV SYNDICATE_UG_HTTP_HOME $HOME/syndicate-ug-http/server

WORKDIR $SYNDICATE_UG_HTTP_HOME
RUN npm install .

COPY server_config.json $SYNDICATE_UG_HTTP_HOME/

WORKDIR $HOME
EXPOSE 38888

CMD node $SYNDICATE_UG_HTTP_HOME/index.js
