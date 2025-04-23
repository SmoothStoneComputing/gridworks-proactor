# Gridworks Proactor

[![PyPI](https://img.shields.io/pypi/v/gridworks-proactor.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/gridworks-proactor.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/gridworks-proactor)][pypi status]
[![License](https://img.shields.io/pypi/l/gridworks-proactor)][license]

[![Read the documentation at https://gridworks-proactor.readthedocs.io/](https://img.shields.io/readthedocs/gridworks-proactor/latest.svg?label=Read%20the%20Docs)][read the docs]
[![Tests](https://github.com/thegridelectric/gridworks-proactor/workflows/Tests/badge.svg)][tests]
[![Codecov](https://codecov.io/gh/thegridelectric/gridworks-proactor/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi status]: https://pypi.org/project/gridworks-proactor/

[read the docs]: https://gridworks-proactor.readthedocs.io/

[tests]: https://github.com/thegridelectric/gridworks-proactor/actions?workflow=Tests

[codecov]: https://app.codecov.io/gh/thegridelectric/gridworks-proactor

[pre-commit]: https://github.com/pre-commit/pre-commit

[black]: https://github.com/psf/black

This packages provides "live actor" and "application monitored communication" infrastructure for the
[GridWorks SpaceHeat SCADA](https://github.com/thegridelectric/gw-scada-spaceheat-python) project. This separation
allows the scada code to be more focussed on on application specific details and provides the potential to re-use the
"live actor" and "application monitored" infrastructure.

## Features

- [](Proactor), a single threaded event loop running on asyncio, for exchanging messages between the main application
  object, "live actor" subobjects and MQTT clients.
- A [communication state] ("active" or not) for each external communications link is available to the proactor and
  sub-objects. "Active" communications is defined as ALL of the following:
    - The underlying communications mechanism (MQTT) is connected.
    - All input channels of underlying mechanism (MQTT topics) are established.
    - A application messages requiring acknowledgement have been ACKed in timely fashion (by default 5 seconds).
    - A message has been received "recently" (by default within 1 minute).
- Reliable delievery of "Events" generated locally. Generated Events are stored locally until they are acknowledged
  and unacknowledged Events are retransmitted when the "Active" communication state is restored.
- [](gwproactor_test), a test package for development and test environments of projects that implement a class derived
  from [](Proactor), allowing the derived class to be tested with the base-class tests.

## Requirements

### Mosquitto

Testing requires an MQTT broker. The Mosquitto broker can be installed with:

```shell
brew install mosquitto
brew services restart mosquitto
```

### TLS

Testing uses TLS by default. The tests require the path to the CA certificate and private key used to sign the
certificate
of the MQTT broker. To set up TLS:

Install gridworks-cert (gwcert):

```shell
pipx install gridworks-cert
```

Create a local Certificate Authority:

```shell
gwcert ca create NAME_OF_YOUR_LOCAL_CA
```

Create certificate and key for the Mosquitto MQTT broker:

```shell
gwcert key add --dns localhost mosquitto
```

- **NOTE**: This command will generate a broker certificate that _only_ allow connections to `localhost`. See
  [External connections](#external-connections) below to create a broker certificate which can accept connections from
  external devices.

Find the path to `mosquitto.conf` in the output of:

```shell
brew services info mosquitto -v
```

Modify `mosquitto.conf` with the TLS configuration in [example-test-mosquitto.conf], fixing up the paths with real
absolute paths to certificate, key and CA certificate files. These paths can be found with:

```shell
gwcert ca info
```

Restart the Mosquitto server:

```shell
brew services restart mosquitto
```

Test Mosquitto 'clear' port:

```shell
# in one window
mosquitto_sub -h localhost -p 1883 -t foo
# in another window
mosquitto_pub -h localhost -p 1883 -t foo -m '{"bla":1}'
```

Test Mosquitto TLS port:

```shell
gwcert key add pubsub
# in one window
mosquitto_sub -h localhost -p 8883 -t foo \
     --cafile $HOME/.local/share/gridworks/ca/ca.crt \
     --cert $HOME/.local/share/gridworks/ca/certs/pubsub/pubsub.crt \
     --key $HOME/.local/share/gridworks/ca/certs/pubsub/private/pubsub.pem
# in another window
mosquitto_pub -h localhost -p 8883 -t foo \
     --cafile $HOME/.local/share/gridworks/ca/ca.crt \
     --cert $HOME/.local/share/gridworks/ca/certs/pubsub/pubsub.crt \
     --key $HOME/.local/share/gridworks/ca/certs/pubsub/private/pubsub.pem \
     -m '{"bar":1}'
```

#### Troubleshooting Mosquitto

Mosquitto logging can be enabled in the `mosquitto.conf` file with the lines:

```
log_dest stderr
log_type all
```

To see the console output, stop the Mosquitto service and start it explicitly on the command line:

```shell
brew services stop mosquitto
mosquitto -c /opt/homebrew/etc/mosquitto/mosquitto.conf
```

#### External connections

The broker certificate must be created with the _hostname_ the client will use to connect to it. For example, to create
a broker certificate reachable at `localhost`, `MyMac.local`, `192.168.1.10` and `foo.bar.baz` use the command:

```shell
gwcert key add \
  --dns localhost \
  --dns MyMac.local \
  --dns 192.168.1.10 \
  --dns foo.bar.baz \
  mosquitto
```

#### Pre-existing key files

If CA or Mosquito certificate can key files _already_ exist, their paths can be specified in `mosquitto.conf` as above
and
for the tests with there GWPROACTOR_TEST_CA_CERT_PATH and GWPROACTOR_TEST_CA_KEY_PATH environment variables.

#### Disabling TLS

To disable testing of TLS, modify the the file `tests/.env-gwproactor-test` with:

```
GWCHILD_PARENT_MQTT__TLS__USE_TLS=false
GWPARENT_CHILD_MQTT__TLS__USE_TLS=false
```

## Installation

You can install _Gridworks Proactor_ via [pip] from [PyPI]:

```console
$ pip install gridworks-proactor
```

## Contributing

Contributions are very welcome. In order to develop, do this:

```console
$ poetry install --all-extras
```

To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_Gridworks Proactor_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [@cjolowicz]'s [Hypermodern Python Cookiecutter] template.

[@cjolowicz]: https://github.com/cjolowicz

[pypi]: https://pypi.org/

[hypermodern python cookiecutter]: https://github.com/cjolowicz/cookiecutter-hypermodern-python

[file an issue]: https://github.com/thegridelectric/gridworks-proactor/issues

[pip]: https://pip.pypa.io/

[example-test-mosquitto.conf]: https://github.com/thegridelectric/gridworks-proactor/blob/main/tests/config/example-test-mosquitto.conf

<!-- github-only -->

[license]: https://github.com/thegridelectric/gridworks-proactor/blob/main/LICENSE

[contributor guide]: https://github.com/thegridelectric/gridworks-proactor/blob/main/CONTRIBUTING.md

[communication state]: https://gridworks-proactor.readthedocs.io/en/latest/comm_state.html
