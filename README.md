# track_results.py

A python package to store summaries of the results of multiple runs of one or several ML algorithms,
to eventually select the best algorithms and/or set of meta-parameters.

The information stored includes:

+ algorithm's meta-parameters
+ platform where algorithm was executed
+ key metrics for the results (including timing information)

The results can either be stored in a local mongita database or in a remote mongoDB. The latter
option is particularly useful when algorithms are executed in multiple computers (perhaps
concurrently).

## Features

+ **Flexible Storage**: Support for local storage (using `mongita`) and remote centralized storage (using `MongoDB`).
+ **Metadata Tracking**: Automatically captures algorithm meta-parameters and platform execution details.
+ **Performance Metrics**: Records timing information and key performance metrics.
+ **Pandas Integration**: Easily retrieve results directly into a Pandas DataFrame for analysis.
+ **Powerful Querying**: Filter and sort through experiment runs using a flexible query interface.

## Installation

Create a local environment and install `track_results` and its dependencies using `pip`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install git+https://github.com/hespanha/TrackResults.py
```

For remote storage with MongoDB, a server is required. For detailed setup instructions, please see INSTALL_MONGODB.md.

To enable storing `matplotlib` figures, you can install the optional dependency:
```bash
pip install "track_results[matplotlib]"
```

## Basic Usage

### Storing Results

```python
from track_results import TrackResults
import my_secrets  # File containing your connection strings (e.g., MONGODB_URI)

# Initialize the tracker. 
# Use a MongoDB URI for remote storage or leave it empty for a local database.
tracker = TrackResults(uri=my_secrets.MONGODB_URI, collection="track_results_collection")

# Define the parameters for your experiment
parameters = {
        "class":"train_neural_network",
        "seed": 42,
        "input_size": 1,
        "n_average_samples": n_average_samples,
        "batch_size": batch_size,
        "num_iterations": 25000,
        "stochastic_hidden": stochastic_hidden,
        "optimizer": {
            "type": "Adam",
            "weight_decay": 0,
            "lr": lr,
        },
    }

# Execute your training function
results = trains_neural_network(*parameters)

# Record the parameters and training results in the database
tracker.add(
    parameters=parameters, 
    results=results, 
    replace=True, # replace existing records with the same parameters if they exist
    )
```

Notes:
*   The `collection` argument specifies the name of the MongoDB collection. If it does not exist, it will be created automatically.
*   The `parameters` and `results` dictionaries can contain nested dictionaries, which are stored hierarchically.
*   The `add()` method automatically adds `date` and `platform` fields to the document.
*   The `replace` argument allows you to overwrite existing records that match the same `parameters` and `platform`. If `False`, a new document is always added.

### Retrieving Results

```python
# Retrieve results from the tracker as a formatted Pandas DataFrame
from IPython import display
df = tracker.get(
        # Filter results using a MongoDB dictionary
        filter={
            "parameters.seed":42,
            "parameters.batch_size": {"$gte": 64}, 
            "results.loss": {"$lt": 0.5}},
        # Select and rename columns for display using a dictionary
        columns={
            "parameters_batch_size": "batch",
            "parameters_num_iterations": "iters",
            "parameters_stochastic_hidden": "hidden",
            "parameters_optimizer_lr": "lr",
            "results_loss": "loss",
            "results_time": "time"
        },
        drop_constant_columns=True,   # Remove columns where values don't change
        sort_by_columns=True,         # Automatically sort by the columns in `columns`
        allow_duplicate_replacements=True,
        exclude_fields=["platform"],  # Do not show the platform field in the output DataFrame
    )
# Display the results in an interactive environment (like Jupyter)
display(df)
```

Notes:
*   The `filter` argument uses MongoDB's query syntax, including dot notation (e.g., `"parameters.optimizer.lr"`) to query nested fields.
*   The `get()` method returns a flattened Pandas DataFrame, where nested keys are joined with an underscore (`_`). For example, a parameter stored as `parameters.optimizer.lr` becomes a column named `parameters_optimizer_lr`.
*   The `columns` argument allows you to select and rename columns for a cleaner output.
*   `drop_constant_columns` helps focus on the variables that change between experiments.
*   `sort_by_columns` sorts the DataFrame based on the order of columns you provide.
*   `exclude_fields` is useful for omitting large or irrelevant top-level fields (like `platform`) from the database query itself, improving performance.

## Advanced Features

### Hierarchical Data Model

By default, `track-results` stores data hierarchically, preserving your nested `parameters` and `results` dictionaries. This allows you to use MongoDB's powerful dot notation for querying deep into your data structures directly at the database level.

When you call `tracker.get()`, the results are returned as a flattened Pandas DataFrame for convenient analysis.

For example, the nested `optimizer` dictionary in the basic usage example:
```python
"optimizer": {
    "type": "Adam",
    "lr": 0.001,
},
```
...can be queried directly using its flattened key:
```python
df = tracker.get(
    columns={"parameters_optimizer_lr": "learning_rate"},
)
```

### Storing Matplotlib Figures

You can store `matplotlib` figures directly in your database in two ways: as a static PDF for archival, or as a pickled object that can be reloaded and modified.

#### Saving Figures as Static Images (PDF)

This method is ideal for storing a final version of a plot for viewing.

```python
import matplotlib.pyplot as plt
from track_results import savefig_to_binary, binary_to_pdf

# Create a figure
fig, ax = plt.subplots()
ax.plot([1, 2, 3], [1, 4, 9])
ax.set_title("My Plot")

# Convert to binary and add to results
results = {
    "loss": 0.123,
    "my_plot": savefig_to_binary(fig)
}
tracker.add(parameters=parameters, results=results)

# Later, to retrieve and save the plot to a file:
df = tracker.get(filter=...)
figure_binary = df.iloc[0]['results_my_plot']
binary_to_pdf(figure_binary, "recovered_plot.pdf")
```

#### Saving Figures as Modifiable Objects (Pickle)

This method serializes the live figure object, allowing you to load it back into Python, make changes (e.g., update titles, add data), and re-display it.

> **Warning**: Unpickling data from an untrusted source is a security risk. Only use this feature on data that your application has generated.

```python
from track_results import savefig_pickle2binary, pickle2binary_to_fig

# ... create fig ...

# Serialize with pickle and add to results
results = {"pickled_plot": savefig_pickle2binary(fig)}
tracker.add(parameters=parameters, results=results)

# Later, to retrieve, modify, and display the plot:
df = tracker.get(filter=...)
retrieved_binary = df.iloc[0]['results_pickled_plot']
loaded_fig = pickle2binary_to_fig(retrieved_binary)

# Modify the loaded figure
loaded_fig.axes[0].set_title("Modified Title")
loaded_fig.show() # Or plt.show() to display
```

### Removing Results

All results from a collection can be removed with

```python
tracker = TrackResults(uri=my_secrets.MONGODB_URI, collection="track_results_collection")
tracker.drop(simulate=False)
```

More often, we may want to remove just a few results:

```python
tracker = TrackResults(uri=my_secrets.MONGODB_URI, collection="track_results_collection")
tracker.remove(
    filter={
        "class":"train_neural_network",
        "parameters_batch_size": {"$gte": 64}, 
        "results_loss": {"$lt": 0.5}},
    simulate=False)
```
