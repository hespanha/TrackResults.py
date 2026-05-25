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

- **Flexible Storage**: Support for local storage (using `mongita`) and remote centralized storage (using `MongoDB`).
- **Metadata Tracking**: Automatically captures algorithm meta-parameters and platform execution details.
- **Performance Metrics**: Records timing information and key performance metrics.
- **Pandas Integration**: Easily retrieve results directly into a Pandas DataFrame for analysis.
- **Powerful Querying**: Filter and sort through experiment runs using a flexible query interface.

## Basic Usage

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
    tracker.add(parameters=parameters, results=results, replace=True)

    # Retrieve results from the tracker as a formatted Pandas DataFrame
    from IPython import display
    df = tracker.get(
            # Filter results using a MongoDB dictionary
            filter={
                "parameters_seed":42,
                "parameters_batch_size": {"$gte": 64}, 
                "results_loss": {"$lt": 0.5}},
            # Select and rename columns for display using a dictionary
            columns={
                "parameters_batch_size": "batch",
                "parameters_num_iterations": "iters",
                "parameters_stochastic_hidden": "hidden",
                "parameters_optimizer_lr": "lr",
                "results_loss": "loss",
                "results_time": "time"
            },
            drop_constant_columns=True,  # Remove columns where values don't change
            sort_by_columns=True,         # Automatically sort by the provided columns
            allow_duplicate_replacements=True,
        )
    # Display the results in an interactive environment (like Jupyter)
    display(df)
    ```

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

## Installation

Create a local environment and install track_results and its dependencies using `pip`

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install git+https://github.com/hespanha/TrackResults.py
    ```

## Install MondoDB (optional)

When using MondoDB, a server needs to be available. A MondoDB server can be installed as follows:

### Linux [install](https://www.mongodb.com/docs/v7.0/administration/install-on-linux/)

1) Install MongoDB PGP

        ```bash
        sudo apt-get update
        sudo apt-get install gnupg curl
        curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
        sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
        curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
        sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
        ```

    for other servers see [pgp](https://www.mongodb.org/static/pgp/)

2) Create file list

    Check release version

        ```bash
        lsb_release -a
        ```

    Create file list using

    for 20.04 (Focal)

        ```bash
        echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
        ```

    for 22.02 (Jammy)

        ```bash
        echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
        ```

    for 22.04 (noble)

        ```bash
        echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
        ```

    For other Ubuntu distributions see [distributions](https://repo.mongodb.org/apt/ubuntu/dists/)

3) Reload package database and install server

        ```bash
        sudo apt-get update
        sudo apt-get install mongodb-org
        ```

4) Configure server (if needed)

    By default, MongoDB runs using the `mongodb` user account

    The official MongoDB package includes a configuration file (`/etc/mongod.conf`). with default
    parameters, including:

        ```yaml
        storage:
        dbPath: /var/lib/mongodb

        systemLog:
        destination: file
        logAppend: true
        path: /var/log/mongodb/mongod.log

        net:
        port: 27017
        bindIp: 127.0.0.1
        ```

    More details at
    [mongo.conf](https://www.mongodb.com/docs/v8.0/reference/configuration-options/)

5) Start server

        ```bash
        sudo systemctl status mongod
        sudo systemctl start mongod
        ```

6) Create admin user

        ```bash
        mongosh
        ```

    Check version (8.0.19?)

        ```mongosh
        db.version()
        ```

    Switch to admin database and create administrator user

        ```mongosh
        use admin
        db.createUser(
        {
            user: "superadmin",
            pwd: passwordPrompt(),
            roles: [ { role: "userAdminAnyDatabase", db: "admin" }, { role: "readWriteAnyDatabase", db: "admin" } ]
        }
        )
        exit
        ```

7) Set authentication & allow to liston from outside

        ```bash
        sudo vi /etc/mongod.conf
        ```

    add

        ```yaml
        security:
        authorization: "enabled"
        net:
        port: 27017
        bindIp: 0.0.0.0
        ```

    restart server

        ```bash
        sudo systemctl restart mongod
        ```

    Verify MongoDB is listening on the correct IP/Port:

        ```bash
        netstat -an | grep 27017
        ```

8) Open firewall

    check firewall status

        ```bash
        sudo ufw status
        ```

    open for

        ```bash
        sudo ufw allow 27017/tcp
        ```

9) Create users

        ```bash
        mongosh -u "superadmin" -p --authenticationDatabase "admin"
        ```

        ```mongosh
        use track_results
        db.createUser(
        {
            user: "hespanha",
            pwd: passwordPrompt(),
            roles: [ { role: "readWrite", db: "track_results" } ]
        }
        )
        db.changeUserPassword("hespanha", passwordPrompt())
        ```

        ```bash
        mongosh -u "hespanha" -p --authenticationDatabase "track_results"
        ```

10) Connect from an application

        ```url
        mongodb://hespanha:appPassword@lambda-dual.ece.ucsb.edu:27017/track_results
        ```

#### Access from python

1) Install PyMongo

        ```bash
        source activate
        pip install pymongo
        ```

2) connect & create record

        ```python
        import pymongo
        from pymongo import MongoClient

        CONNECTION_STRING = "mongodb://hespanha:????password????@lambda-dual.ece.ucsb.edu:27017/track_results"
        CONNECTION_STRING = "mongodb://hespanha:????password????@localhost:27017/track_results" 

        client = MongoClient(CONNECTION_STRING)
        db = client['track_results']
        collection = db['atlatl']

        # write records
        hello_world_document = {
            "message": "Hello, World!",
            "language": "Python",
            "database": "MongoDB"
        }
        result = collection.insert_one(hello_world_document)
        hello_world_document = {
            "message": "Second Hello, World!",
            "language": "Python",
            "database": "MongoDB"
        }
        result = collection.insert_one(hello_world_document)
        client.close()
        ```

3) connect and read record

        ```python
        import pymongo
        from pymongo import MongoClient

        CONNECTION_STRING = "mongodb://hespanha:????password????@localhost:27017/track_results" 
        CONNECTION_STRING = "mongodb://hespanha:????password????@lambda-dual.ece.ucsb.edu:27017/track_results"

        client = MongoClient(CONNECTION_STRING)
        db = client['track_results']
        collection = db['atlatl']

        # read all records
        all_records_cursor = collection.find({})

        print("All records found:")
        for record in all_records_cursor:
            print(record)
        ```

4) connect and remove all records from a collection

        ```python
        import pymongo
        from pymongo import MongoClient

        CONNECTION_STRING = "mongodb://hespanha:????password????@localhost:27017/track_results" 
        CONNECTION_STRING = "mongodb://hespanha:????password????@lambda-dual.ece.ucsb.edu:27017/track_results"

        client = MongoClient(CONNECTION_STRING)
        db = client['track_results']
        collection = db['atlatl']

        collection.delete_many({})

        # or 

        collection.drop()
        ``
