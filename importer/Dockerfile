FROM node:0.12
MAINTAINER Ben Booth <bkbooth@gmail.com>

# Setup application directory
ENV APP_DIR /usr/src/app/
RUN mkdir -p $APP_DIR
WORKDIR $APP_DIR
VOLUME $APP_DIR

# Copy all application files
COPY . $APP_DIR

# Install node dependencies
RUN npm install && \
    npm cache clear

CMD npm start
