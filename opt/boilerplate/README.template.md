# Boilerplate

This is a new project: boilerplate.

## Getting started

```
poetry install
poetry run boilerplate --help
```


## Configuration

**DIRECTORIES:** The project assumes a `/conf` and a `/data` directory
inside this folder. You can overwrite the location of the data
directory with `BOILERPLATE_DATA`.

**LOGGING:** There several customization options. Logging can be
adjusted in `/conf/logging.yaml`. You can also overwrite
`BOILERPLATE_LOG_FILE` to adjust the log file name or
`BOILERPLATE_LOG_CONF` to provide a completely separate log
configuration. For nice colouring, a multitail configuration is
provided in `/conf`.
