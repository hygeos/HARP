
<!-- <div align="center"> -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/logo_dark.svg" width="300">
  <source media="(prefers-color-scheme: light)" srcset="img/logo_light.svg" width="300">
  <img alt="HARP project logo">
</picture>
<!-- </div> -->

# Harmonized Ancillary Resource Provider

[**Quickstart**](#quickstart-colab-in-the-cloud)
| [**Install guide**](#installation)
| [**Transformations**](#transformations)

## What is HARP ?

Harp is an harmonisation layer for the different geospatial data providers. It also acts as a proxy for executing the API requests. It allow to standardize the way of querying and accessing data, despite the various differences between the providers.
For example it can querry data from ECMWF, or from NASA. Currently the project interfaces with ERA5, CAMS global athmospheric composition forecast and MERRA2

HARP has been developped as an internal tool for HYGEOS, and is still in development.

## Why use HARP ?

- Abstracted query for data
- Cached source data
- Harmonized xarray objects

HARP facilitate the development of generic algorithms, because it standardizes both the request and the output data. 

## Quickstart

### ECMWF
You will need an ECMWF account, furthermore you will need to accept the EULAs forms for each product you want to download. This is done from your account.

Afterward you will need to set the url + key for each product, in the file .cdsapi in your home folder.

### Nasa
You will need an earthdata account to query MERRA2 data, which needs to be set in the .netrc file from your home folder.

## Installation
The package can be installed with the command:
```sh
pip install git+https://github.com/hygeos/HARP.git
```

## Extending HARP

HARP is modular and conceived to be extended when required. New providers can be added by creating a new class and inheriting BaseProvider, new products can be added inside the existing Providers class

