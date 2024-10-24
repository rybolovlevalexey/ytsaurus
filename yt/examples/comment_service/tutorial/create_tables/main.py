# -*- coding: utf-8 -*-
import yt.wrapper as yt
import argparse


# In order to make it easier to create the necessary set of tables during testing,
# a generalized create_database function is allocated, which will take the function of creating a separate
# table as an argument
def create_database(create_table):
    # Creating a topic_comments table
    # To make a column a key one, you must specify the sort_order attribute in the schema (only ascending is supported)
    # To make a column aggregating, you must specify the aggregation function in the schema
    create_table(
        name="topic_comments",
        schema=[
            {"name": "topic_id", "type": "string", "sort_order": "ascending"},
            {"name": "parent_path", "type": "string", "sort_order": "ascending"},
            {"name": "comment_id", "type": "uint64"},
            {"name": "parent_id", "type": "uint64"},
            {"name": "user", "type": "string"},
            {"name": "creation_time", "type": "uint64"},
            {"name": "update_time", "type": "uint64"},
            {"name": "content", "type": "string"},
            {"name": "views_count", "type": "int64", "aggregate": "sum"},
            {"name": "deleted", "type": "boolean"},
        ],
    )

    # Creating a user_comments table
    # To make a column computable, you must specify an expression from other key columns in the schema,
    # which will be written in it
    create_table(
        name="user_comments",
        schema=[
            {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(user)"},
            {"name": "user", "type": "string", "sort_order": "ascending"},
            {"name": "topic_id", "type": "string", "sort_order": "ascending"},
            {"name": "parent_path", "type": "string", "sort_order": "ascending"},
            {"name": "update_time", "type": "uint64"},
        ],
    )

    # Creating topics table
    create_table(
        name="topics",
        schema=[
            {"name": "topic_id", "type": "string", "sort_order": "ascending"},
            {"name": "comment_count", "type": "uint64", "aggregate": "sum"},
            {"name": "update_time", "type": "uint64"},
        ],
    )


# Function for creating production data base
# For flexibility, the create_replicated_table function sent to create_database,
# takes a minimum of arguments, namely, the name and the scheme, and everything else is taken from the external context
def create_production_database(path, meta_cluster, replica_clusters, force):
    def create_replica(table_path, replica_path, replica_cluster, schema):
        # To interact with clusters, an RPC proxy is used, which is specified in the config
        meta_client = yt.YtClient(meta_cluster, config={"backend": "rpc"})
        replica_client = yt.YtClient(replica_cluster, config={"backend": "rpc"})
        if force:
            replica_client.remove(replica_path, force=True)

        # Creating a replica object on a meta cluster: links to the replica table that should appear on the replica_cluster cluster
        replica_id = meta_client.create("table_replica", attributes={
            "table_path": table_path,
            "cluster_name": replica_cluster,
            "replica_path": replica_path,
        })

        # Creating a replica table
        # The SSD medium will be used for data storage, as indicated by the primary_medium attribute
        replica_client.create("table", replica_path, ignore_existing=True, attributes={
            "schema": schema, "dynamic": True,
            "upstream_replica_id": replica_id,
            "primary_medium": "ssd_blobs",
        })
        # It is necessary to mount the tables immediately in order to be able to read and write data
        # The sync=True parameter allows you to synchronously wait for the table to be mounted
        replica_client.mount_table(replica_path, sync=True)
        # Setting the table replication mode
        meta_client.alter_table_replica(replica_id, True, mode="async")


    def create_replicated_table(name, schema):
        table_path = "{}/{}".format(path, name)
        replica_path = "{}/{}_replica".format(path, name)

        # To interact with clusters, an RPC proxy is used, which is specified in the config
        client = yt.YtClient(meta_cluster, config={"backend": "rpc"})
        # Handling the case of the existence of a table
        if client.exists(table_path):
            if not force:
                print("Replicated table {} at cluster {} already exists; use option --force to recreate it".format(
                    table_path, meta_cluster
                ))
                return None
            client.remove(table_path, force=True)

        # Creating a replicated dynamic table
        # The SSD medium will be used for data storage, as indicated by the primary_medium attribute
        # It is worth enabling automatic synchronous replica switching
        # using the enable_replicated_table_tracker parameter:
        # then one of the replicas will automatically be kept synchronous
        client.create("replicated_table", table_path, attributes={
            "schema": schema, "dynamic": True,
            "primary_medium": "ssd_blobs",
            "replicated_table_options": {"enable_replicated_table_tracker": True},
        })
        # It is necessary to mount tables immediately in order to read and write data
        # The sync=True parameter allows you to synchronously wait for the table to be mounted
        client.mount_table(table_path, sync=True)

        # Creating asynchronous replics
        for replica_cluster in replica_clusters:
            create_replica(table_path, replica_path, replica_cluster, schema)

    create_database(create_replicated_table)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--path", type=str)
    parser.add_argument("--meta_cluster", type=str)
    parser.add_argument("--replica_clusters", nargs='*')
    parser.add_argument("--force", action="store_true")
    params = parser.parse_args()

    create_production_database(params.path, params.meta_cluster, params.replica_clusters, params.force)


if __name__ == "__main__":
    main()
