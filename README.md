# ReSkein

> JVM-free Python library and command-line tool for deploying applications on Apache YARN using YARN's web services REST API. It can be used as a standalone library / CLI or as a patch for the jcrist's [Skein](https://github.com/jcrist/skein) library.

## Highlights

* **JVM-free**: No need to install Java or any JVM-based software.
* **Standalone or Patch**: Can be used as a standalone library or as a patch for the jcrist's [Skein](https://github.com/jcrist/skein) library.
* **Easy Deployment**: Declaritive API (YAML or Python) for deploying applications via YARN's web services REST API.

> **Note**: For further details see `Skein`'s [documentation](https://jcristharif.com/skein/).

## Installation

```bash
pip install reskein
```

## Usage

```python
import reskein as rs

# Create a client
client = rs.Client()

# Deploy an application
app = rs.Application(
    name='my-app',
    resources=rs.Resources(memory='1 GiB', vcores=1),
    script='echo "Hello, World!"',
)
client.deploy(app)
```