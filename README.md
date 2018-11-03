# greenbus-edge
Telemetry front end for data acquisition via DNP3, Modbus, etc.

## Build
```
git clone ...
# install dependencies
cd greenbus-edge
mvn install
```

### Dependencies

#### Protoc
* Install Protoc **Version 3.1** (or the version specified in root pom.xml protobuf-java).
* ln -s \`which protoc\` protoclink

*Linux Install*
```
# ? install instructions ?
ln -s `which protoc` protoclink
```

*MacOS Install*
```
brew install protobuf@3.1
cd greenbus-edge
# Older version is not linked to /usr/local/bin/protoc, so it's not in the path.
sudo ln -s /usr/local/opt/protobuf\@3.1/bin/protoc /usr/local/bin/protoc
ln -s `which protoc` protoclink
```
