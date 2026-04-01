# How to publish the package

1) Build

        ```bash
        pip install --upgrade build twine
        python -m build
        ```

2) Test installation locally

        ````bash
        deactivate
        python -m venv testenv
        source testenv/bin/activate 
        pip install ./dist/track_results-0.1.0-py3-none-any.whl

        python -c "import test_results; print('Success!')"
        ```