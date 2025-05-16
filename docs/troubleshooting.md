# Troubleshooting

## Common errors

### Python version

```zsh
$ pip3 install salesforce-data-customcode
ERROR: Ignored the following versions that require a different python version: 0.1.0 Requires-Python <3.12,>=3.10; 0.1.2 Requires-Python <3.12,>=3.10
ERROR: Could not find a version that satisfies the requirement salesforce-data-customcode (from versions: none)
```

You are not using a supported version of python.  For example, this package does not support version `3.9.6`.  You must upgrade to 3.11, or use
[pyenv](https://github.com/pyenv/pyenv) to configure 3.11.

### JDK version

```zsh
pyspark.errors.exceptions.base.PySparkRuntimeError: [JAVA_GATEWAY_EXITED] Java gateway process exited before sending its port number.
```

This error indicates your machine either doesn't have a JDK configured, or an incompatible version of JDK is configured (e.g. 11).

Ensure that:
- `JAVA_HOME` is setup properly and you can run `java -version` successfully
- You have openJDK zulu 17 installed: https://www.azul.com/downloads/?package=jdk#zulu

### Docker not running

```zsh
return code: 1
stdout:
stderr:
    Cannot connect to the Docker daemon at unix:///Users/username/.docker/run/docker.sock. Is the docker daemon running?
```

This indicates that, even if you have docker installed, it's not running.  [Docker Desktop](https://docs.docker.com/desktop/)
is an easy way to use docker on your machine.  After installation, make sure to double-click the app in Applications folder
(if on mac) so that it's running.
