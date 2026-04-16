# How to publish the package

## Initialize project

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -e . # searches for pyproject.toml file
    ```

## Publish

1) Build

        ```bash
        pip install --upgrade build twine
        python -m build
        ```

    Attention: make sure "version" in pyproject.toml macthes "__version__" in __init__.py

2) Test installation locally

        ````bash
        deactivate
        rm -r testenv
        python -m venv testenv
        source testenv/bin/activate 
        pip install ./dist/track_results-0.1.0-py3-none-any.whl
        python -c "import track_results; print('Success')"
        rm -r testenv
        deactivate
        ```

3) Test installation from GitHub

        ````bash
        deactivate
        rm -r testenv
        python -m venv testenv
        source testenv/bin/activate 
        pip install git+https://github.com/hespanha/TrackResults.py
        python -c "import track_results; print('Success')"
        deactivate
        rm -r testenv
        ```
