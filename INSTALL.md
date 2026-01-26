# Installation

## Index

- [Homebrew](#homebrew)
- [Debian-based distributions](#debian-based-distributions)
- [RPM-based distributions](#rpm-based-distributions)
- [Prebuilt Binary](#prebuilt-binary)
- [Golang](#golang)

## Homebrew

```sh
brew install cirruslabs/cli/omni-cache
```

## Debian-based distributions

Firstly, make sure that the APT transport for downloading packages via HTTPS and common X.509 certificates are installed:

```sh
sudo apt-get update && sudo apt-get -y install apt-transport-https ca-certificates
```

Then, add the Cirrus Labs repository:

```sh
echo "deb [trusted=yes] https://apt.fury.io/cirruslabs/ /" | sudo tee /etc/apt/sources.list.d/cirruslabs.list
```

Finally, update the package index files and install Omni Cache:

```sh
sudo apt-get update && sudo apt-get -y install omni-cache
```

## RPM-based distributions

First, create a `/etc/yum.repos.d/cirruslabs.repo` file with the following contents:

```
[cirruslabs]
name=Cirrus Labs Repo
baseurl=https://yum.fury.io/cirruslabs/
enabled=1
gpgcheck=0
```

Then, install Omni Cache:

```sh
sudo yum -y install omni-cache
```

## Prebuilt Binary

Check the [releases page](https://github.com/cirruslabs/omni-cache/releases) for a pre-built `omni-cache` binary for your platform.

Here is a one liner for Linux/macOS to download the latest release and add it to your PATH:

```sh
curl -L -o omni-cache https://github.com/cirruslabs/omni-cache/releases/latest/download/omni-cache-$(uname | tr '[:upper:]' '[:lower:]')-amd64 \
  && sudo mv omni-cache /usr/local/bin/omni-cache && sudo chmod +x /usr/local/bin/omni-cache
```

Replace `amd64` with `arm64` if needed.

## Golang

If you have the latest [Golang](https://golang.org/) installed locally, you can run:

```sh
go install github.com/cirruslabs/omni-cache/cmd/omni-cache@latest
```

This will build and place the `omni-cache` binary in `$GOPATH/bin`.

To be able to run `omni-cache` from anywhere, make sure the `$GOPATH/bin` directory is added to your `PATH`
environment variable (see [article in the Go wiki](https://github.com/golang/go/wiki/SettingGOPATH) for more details).
