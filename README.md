# Analytics support

A Python verison of the analytics support library.

## ZScaler certificate issues.

When using `pip`, always add the following command line parameters:

```sh
pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org <package>
```

## Packaging

To install in development mode:

```sh
pip install -e .
```

To create a source distribution

```sh
python setup.py sdist
```

To build the Wheel package

```sh
python setup.py bdist_wheel --universal
```

To do a local install of a wheel package

```sh
pip install --force-reinstall --no-deps dist/*.whl
```

To uninstall:
```sh
pip uninstall -y analytics-common
```

## Unit testing

```sh
python -m unittest discover -v
```
