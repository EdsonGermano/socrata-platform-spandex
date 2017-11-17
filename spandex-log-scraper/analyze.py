import argparse
import logging
import os
import re
from collections import namedtuple
from datetime import datetime
from functools import partial
from itertools import chain
from urllib.parse import parse_qs

import pandas as pd
import simplejson as json


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract JSON request logs into single DataFrame and report some statistics")

    parser.add_argument("--fxf_domain_map_file", required=True,
                        help="A tab delimited file mapping FXFs to domains")
    parser.add_argument("--dataset_id_fxf_map_file", required=True,
                        help="A tab delimited file mapping dataset IDs to FXFs")
    parser.add_argument("--spandex_dataset_ids", required=True,
                        help="A file containing the set of dataset IDs currently indexed in Spandex")
    parser.add_argument("--logfile", action="append")
    parser.add_argument("--logfile_dir", help="A directory containing spandex request log files")
    parser.add_argument("--log_dataframe",
                        help="Instead of reading in raw logs, read previously extracted logs from "
                        "a pickled DataFrame")
    parser.add_argument("--zero_request_datasets", default="spandex_zero_request_datasets.txt",
                        help="A path to a text file containing data the IDs for the datasets that "
                        "received 0 requests")
    parser.add_argument("--output_file", help="Where to write the resulting DataFrame",
                        default="spandex_logs_df.pickle")

    return parser.parse_args()


DatasetFxfVersionPair = namedtuple("DatasetFxfVersionPair", ["fxf", "version"])


def dataset_id_fxf_map_from_file(input_file):
    """
    Read in a tab-delimited file with dataset system IDs, FXFs, and version numbers.

    Args:
        input_file (str): The path to the tab-delimited metadata file

    Returns:
        A dictionary mapping dataset system IDs to `DatasetFxfVersionPair`
    """
    with open(input_file, "r") as infile:
        data = (tuple(line.strip().split('\t')) for line in infile if line.strip())
        return {t[0]: DatasetFxfVersionPair(*t[1:]) for t in data}


Domain = namedtuple("Domain", ["cname", "salesforce_id", "deleted_at"])


def is_customer_domain(domain):
    """
    Determine whether a domain is a customer domain.

    Args:
        domain (Domain): A Domain

    Returns:
        A boolean indicating whether the domain is a customer domain.

    Note:
        These conditions are not yet perfect indicators, but close enough for now.
    """
    _, salesforce_id, deleted_at = domain
    return salesforce_id and not domain.deleted_at


def fxf_domain_map_from_file(input_file):
    """
    Read in a tab-delimited file with lens FXFs and domain metadata.

    Args:
        input_file (str): The path to the tab-delimited metadata file

    Returns:
        A dictionary mapping lens FXFs to `Domain`
    """
    def pad(elems):
        return elems + ([None] * (4 - len(elems)))

    with open(input_file, "r") as infile:
        data = (tuple(pad(line.strip().split('\t'))) for line in infile if line.strip())
        return {t[0]: Domain(*t[1:]) for t in data}


REQUEST_PATH_RE = re.compile(
    r"GET /suggest/"
    r"(?P<dataset_id>(?:alpha|bravo)\.[0-9]+)/"
    r"(?P<pub_stage>[A-Za-z0-9]+)/"
    r"(?P<column_id>[0-9a-z]{4}-[0-9a-z]{4})"
    r"(\?(?P<query_params>[^ ]+))?")


def merge(dict1, dict2):
    """
    Merge two dictionaries.

    Args:
        dict1 (dict): The first dictionary
        dict2 (dict): The second dictionary
    """
    return {**dict1, **dict2}


def dataset_metadata(dataset_id, dataset_id_fxf_map, fxf_domain_map):
    """
    Return dataset metadata as a dictionary.

    Args:
        dataset_id (str): The dataset system ID
        dataset_id_fxf_map (dict): A map of dataset IDs to DatasetFxfVersionPairs
        fxf_domain_map (dict): A map of dataset FXFs to NameDomainPairs

    Returns:
        A dictionary with some additional dataset metadata
    """
    fxf, version = dataset_id_fxf_map.get(dataset_id, (None, None))
    domain = fxf_domain_map.get(fxf, (None, None, None))
    cname, salesforce_id, deleted_at = domain
    is_customer_domain_ = is_customer_domain(domain)

    return {
        "dataset_id": dataset_id,
        "fxf": fxf,
        "version": version,
        "domain": cname,
        "salesforce_id": salesforce_id,
        "domain_deleted_at": deleted_at,
        "is_customer_domain": is_customer_domain_
    }


def extract(message, dataset_id_fxf_map, fxf_domain_map):
    """
    Extract useful bits of information from a log message.

    Args:
        message (dict): The raw message dict (should have _raw and _messagetime fields)
        dataset_id_fxf_map (dict): A map of dataset IDs to DatasetFxfVersionPairs
        fxf_domain_map (dict): A map of dataset FXFs to NameDomainPairs

    Returns:
        A dictionary with some additional info
    """
    if not (message.get("_raw") and message.get("_messagetime")):
        logging.warn(
            "Skipping record with no _raw field and no _messagetime field: {}".format(message))
        return None

    # NOTE: sumo logic is padding that timestamp with zeroes, but slicing to 10 seems wrong
    messagetime = datetime.fromtimestamp(int(message["_messagetime"][:10]))
    m = REQUEST_PATH_RE.search(message["_raw"])

    if not m:
        logging.warn(
            "Skipping record that doesn't match request pattern: {}".format(message["_raw"]))
        return None

    request_parts = m.groupdict()
    query_params = parse_qs(request_parts.get("query_params"))
    dataset_id = request_parts.get("dataset_id")
    pub_stage = request_parts.get("pub_stage")

    request_data = {
        "messagetime": messagetime,
        "query_params": query_params,
        "dataset_id": dataset_id,
        "pub_stage": pub_stage
    }

    # pull in some extra fields for completeness
    metadata = dataset_metadata(dataset_id, dataset_id_fxf_map, fxf_domain_map)

    return merge(metadata, request_data)


def read_log_file(input_file):
    """
    Read a JSON lines file of request logs.

    Args:
        input_file (str): The path to a JSON lines file with Spandex logs as outputted by Sumo
            (like those generated by the `scrape_logs.py` script)

    Returns:
        A list of dictionaries where each dict corresponds to a request log entry
    """
    logging.info("Reading request logs from {}".format(input_file))
    return [json.loads(line.strip()) for line in open(input_file, "r") if line.strip()]


def spandex_dataset_ids(input_file):
    """
    Read all dataset IDs currently in the Spandex index.

    Args:
        input_file (str): The path to a flat file with a list of dataset IDs currently in spandex

    Returns:
        A set of dataset IDs

    Note:
        The Spandex index is extremely sensitive to expensive queries. If you change this, be
        mindful of query performance and monitor cluster health when you run it.
    """
    return {x.strip() for x in open(input_file, "r") if x.strip()}


def count_field_values(es_client):
    """
    Count the number of field values in the Spandex index.

    Args:
        es_client (clients_common.ElasticsearchClient): An Elasticsearch client

    Returns:
        The number of documents in the field_values type of the Spandex index
    """
    result = es_client.conn_es.search(index=es_client.index, doc_type="field_value", size=0)
    return result["hits"]["total"]


def enrich_data_from_spandex(datasets, dataset_id_fxf_map, fxf_domain_map):
    """
    Enrich dataset IDs from Spandex with additional metadata.

    Args:
        dataset_id_fxf_map (dict): A map of dataset IDs to DatasetFxfVersionPairs
        fxf_domain_map (dict): A map of dataset FXFs to NameDomainPairs

    Returns:
        A dictionary with some additional info
    """
    return [dataset_metadata(dataset_id, dataset_id_fxf_map, fxf_domain_map)
            for dataset_id in datasets]


def main():
    # setup logging
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    # parse command-line args
    args = parse_args()

    # read in some extra dataset metadata
    dataset_id_fxf_map = dataset_id_fxf_map_from_file(args.dataset_id_fxf_map_file)
    fxf_domain_map = fxf_domain_map_from_file(args.fxf_domain_map_file)

    # read spandex dataset information
    spandex_datasets = spandex_dataset_ids(args.spandex_dataset_ids)
    print("Found {} datasets in the Spandex index".format(len(spandex_datasets)))

    # read request logs either from previously pickled DataFrame or JSON lines
    if args.log_dataframe:
        logs_df = pd.read_pickle(args.log_dataframe)
    else:
        log_dir = args.logfile_dir
        log_files = sorted(
            [os.path.join(log_dir, log_file) for log_file in os.listdir(log_dir)],
            reverse=True)
        logging.info("Reading logfiles: {}".format(log_files))
        extract_ = partial(
            extract, dataset_id_fxf_map=dataset_id_fxf_map, fxf_domain_map=fxf_domain_map)
        logs = (extract_(msg) for msg in chain.from_iterable(
            read_log_file(logfile) for logfile in log_files))

        logs_df = pd.DataFrame.from_records(record for record in logs if record)

    print("Found {} suggest requests".format(len(logs_df)))
    pd.options.display.max_rows = 1000

    dataset_counts = logs_df["dataset_id"].value_counts()
    print("Top datasets by request count")
    print(dataset_counts.head(100))

    num_unique_datasets = len(logs_df["dataset_id"].unique())
    print("Number of unique datasets receiving suggestion requests: {}".format(num_unique_datasets))

    nonzero_request_datasets = set(logs_df["dataset_id"].unique())
    zero_request_datasets = spandex_datasets - nonzero_request_datasets
    print("{} datasets in Spandex received 0 suggest requests".format(len(zero_request_datasets)))
    with open(args.zero_request_datasets, "w") as outfile:
        for dataset in zero_request_datasets:
            outfile.write(dataset + "\n")

    zero_request_datasets_df = pd.DataFrame(
        enrich_data_from_spandex(zero_request_datasets, dataset_id_fxf_map, fxf_domain_map))
    print("Top domains among datasets receiving zero requests")
    domain_counts = zero_request_datasets_df["domain"].value_counts()
    print(domain_counts.head(100))

    requested_datasets_missing_from_spandex = nonzero_request_datasets - spandex_datasets
    print("{} datasets that are missing from Spandex received one or more suggest requests".format(
        len(requested_datasets_missing_from_spandex)))
    missing_from_spandex_df = logs_df[logs_df["dataset_id"].apply(
        lambda dataset_id: dataset_id in requested_datasets_missing_from_spandex)]
    missing_df = missing_from_spandex_df.groupby("dataset_id")\
                                        .apply(len)\
                                        .reset_index()\
                                        .rename(columns={0: "request_count"})\
                                        .sort_values("request_count", ascending=False)\

    print("Top 20 datasets that are missing from Spandex by request count")
    print(missing_df.head(20))

    spandex_datasets_df = pd.DataFrame(
        enrich_data_from_spandex(spandex_datasets, dataset_id_fxf_map, fxf_domain_map))
    non_customer_domain_datasets_df = spandex_datasets_df[spandex_datasets_df["is_customer_domain"].apply(
        lambda b: b is not None and b == False)]
    print("{} / {} datasets in Spandex are from non-customer domains".format(
        len(non_customer_domain_datasets_df), len(spandex_datasets)))

    if not args.log_dataframe:
        logging.info("Pickling logs dataframe at path {}".format(args.output_file))
        logs_df.to_pickle(args.output_file)


if __name__ == "__main__":
    main()
